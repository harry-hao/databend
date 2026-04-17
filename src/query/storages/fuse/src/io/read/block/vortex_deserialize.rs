// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_select::filter::filter_record_batch;
use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_expression::types::Bitmap;
use databend_storages_common_table_meta::meta::ColumnMeta;
use vortex::arrow::IntoArrowArray;
use vortex::dtype::FieldNames;
use vortex::expr::Expression;
use vortex::expr::root;
use vortex::expr::select;
use vortex::iter::ArrayIteratorExt;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::runtime::single::SingleThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

use super::parquet::RowSelection;
use super::vortex_read_at::OpendalReadAt;
use crate::io::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;
use databend_common_base::runtime::GlobalIORuntime;

impl BlockReader {
    /// Deserialize one FUSE block stored as a single Vortex file (see `encode_data_blocks_as_vortex`).
    pub fn deserialize_vortex_chunks(
        &self,
        block_path: &str,
        num_rows: usize,
        _column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
    ) -> Result<DataBlock> {
        self.deserialize_vortex_chunks_with_scan_filter(
            block_path,
            num_rows,
            _column_metas,
            column_chunks,
            selection,
            None,
            None,
        )
    }

    pub fn deserialize_vortex_chunks_with_scan_filter(
        &self,
        block_path: &str,
        num_rows: usize,
        _column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
        scan_filter: Option<Expression>,
        extra_projection: Option<FieldNames>,
    ) -> Result<DataBlock> {
        let result_rows = selection.map(|s| s.selected_rows).unwrap_or(num_rows);

        if self.projected_schema.fields.is_empty() {
            return Ok(DataBlock::empty_with_rows(result_rows));
        }

        if result_rows == 0 {
            return Ok(DataBlock::empty_with_schema(&self.data_schema()));
        }

        // The merge-IO path provides column-wise byte spans for caching and other formats, but
        // Vortex decoding should avoid requiring the full file bytes. We open the Vortex file via
        // a `VortexReadAt` implementation backed by OpenDAL range reads.
        //
        // NOTE: `column_chunks` is currently unused for Vortex decoding; it will be revisited once
        // we can feed those buffers into Vortex as an optional segment cache.
        let _ = column_chunks;
        let name_paths = column_name_paths(&self.projection, &self.original_schema);
        for column_node in self.project_column_nodes.iter() {
            if column_node.is_nested {
                return Err(ErrorCode::StorageOther(
                    "FUSE storage_format='vortex' nested projection is not supported yet",
                ));
            }
        }

        let top_level_names = name_paths
            .iter()
            .map(|p| p[0].as_str())
            .collect::<Vec<_>>();
        let mut projection_vec = top_level_names
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        if let Some(extra) = extra_projection {
            for f in extra.iter() {
                let f = f.to_string();
                if !projection_vec.iter().any(|x| x == &f) {
                    projection_vec.push(f);
                }
            }
        }
        let projection = FieldNames::from_iter(projection_vec.iter().map(|s| s.as_str()));

        let scan_filter_present = scan_filter.is_some();
        let mut record_batch = decode_vortex_file_to_record_batch(
            self.operator.clone(),
            block_path,
            Some(projection),
            scan_filter,
        )?;

        // When a scan filter is applied, Vortex may return fewer rows than the on-disk footer count.
        let result_rows = record_batch.num_rows();
        if result_rows > num_rows {
            return Err(ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' decoded rows {result_rows} exceeds metadata {num_rows}",
            )));
        }

        if let Some(selection) = selection {
            if scan_filter_present {
                return Err(ErrorCode::BadBytes(
                    "FUSE storage_format='vortex' unexpected combination: scan_filter + row_selection"
                        .to_string(),
                ));
            }
            let predicate = bitmap_to_boolean_array(&selection.bitmap)?;
            record_batch = filter_record_batch(&record_batch, &predicate).map_err(|e| {
                ErrorCode::BadBytes(format!(
                    "FUSE storage_format='vortex' failed to apply row filter: {e}"
                ))
            })?;
        }

        let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
        for ((i, field), _column_node) in self
            .projected_schema
            .fields
            .iter()
            .enumerate()
            .zip(self.project_column_nodes.iter())
        {
            let data_type = field.data_type().into();

            let value = match try_column_by_name(&record_batch, &name_paths[i]) {
                Some(arrow_array) => Value::from_arrow_rs(arrow_array, &data_type)?,
                None => Value::Scalar(self.default_vals[i].clone()),
            };
            entries.push(BlockEntry::new(value, || (data_type, record_batch.num_rows())));
        }
        Ok(DataBlock::new(entries, record_batch.num_rows()))
    }
}

fn decode_vortex_file_to_record_batch(
    operator: opendal::Operator,
    location: &str,
    projection: Option<FieldNames>,
    scan_filter: Option<Expression>,
) -> Result<RecordBatch> {
    let rt = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(rt.handle());
    let file = GlobalIORuntime::instance()
        .block_on(async move {
            let read_at = OpendalReadAt::open(operator, location).await.map_err(|e| {
                ErrorCode::BadBytes(format!(
                    "FUSE storage_format='vortex' failed to build read_at for {location}: {e}"
                ))
            })?;
            session
                .open_options()
                .open_read_at(read_at)
                .await
                .map_err(|e| {
                    ErrorCode::BadBytes(format!(
                        "FUSE storage_format='vortex' failed to open Vortex file via read_at: {e}"
                    ))
                })
        })?;

    let scan = file.scan().map_err(|e| {
        ErrorCode::BadBytes(format!(
            "FUSE storage_format='vortex' failed to begin scan: {e}"
        ))
    })?;

    let scan = match scan_filter {
        Some(filter) => scan.with_filter(filter),
        None => scan,
    };

    let scan = match projection {
        Some(fields) => scan.with_projection(select(fields, root())),
        None => scan,
    };

    let array_ref = scan
        .into_array_iter(&rt)
        .map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to build scan iterator: {e}"
            ))
        })?
        .read_all()
        .map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to read arrays: {e}"
            ))
        })?;

    record_batch_from_vortex_root(array_ref)
}

fn record_batch_from_vortex_root(array_ref: vortex::ArrayRef) -> Result<RecordBatch> {
    if let Ok(rb) = RecordBatch::try_from(array_ref.as_ref()) {
        return Ok(rb);
    }
    let arrow = array_ref.into_arrow_preferred().map_err(|e| {
        ErrorCode::BadBytes(format!(
            "FUSE storage_format='vortex' could not convert root array to Arrow: {e}"
        ))
    })?;
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            ErrorCode::BadBytes(
                "FUSE storage_format='vortex' expected struct-typed root Arrow array".to_string(),
            )
        })?;
    Ok(RecordBatch::from(struct_array.clone()))
}

fn try_column_by_name(record_batch: &RecordBatch, name_path: &[String]) -> Option<ArrayRef> {
    if name_path.is_empty() {
        return None;
    }
    let mut array: ArrayRef = record_batch.column_by_name(&name_path[0])?.clone();
    for name in name_path.iter().skip(1) {
        let struct_array = array.as_any().downcast_ref::<StructArray>()?;
        array = struct_array.column_by_name(name)?.clone();
    }
    Some(array)
}

fn bitmap_to_boolean_array(bitmap: &Bitmap) -> Result<arrow_array::BooleanArray> {
    let values: Vec<bool> = (0..bitmap.len())
        .map(|i| unsafe { bitmap.get_bit_unchecked(i) })
        .collect();
    Ok(arrow_array::BooleanArray::from(values))
}

fn column_name_paths(projection: &Projection, schema: &TableSchema) -> Vec<Vec<String>> {
    match projection {
        Projection::Columns(field_indices) => field_indices
            .iter()
            .map(|i| vec![schema.fields[*i].name().to_string()])
            .collect(),
        Projection::InnerColumns(path_indices) => {
            let mut name_paths = Vec::with_capacity(path_indices.len());
            for index_path in path_indices.values() {
                let mut name_path = Vec::with_capacity(index_path.len());
                let first_index = index_path[0];
                name_path.push(schema.fields[first_index].name().to_string());
                let mut idx = 1;
                let mut ty = schema.fields[first_index].data_type().clone();
                while idx < index_path.len() {
                    match ty.remove_nullable() {
                        TableDataType::Tuple {
                            fields_name,
                            fields_type,
                        } => {
                            let next_index = index_path[idx];
                            name_path.push(fields_name[next_index].clone());
                            ty = fields_type[next_index].clone();
                        }
                        _ => unreachable!(),
                    }
                    idx += 1;
                }
                name_paths.push(name_path);
            }
            name_paths
        }
    }
}
