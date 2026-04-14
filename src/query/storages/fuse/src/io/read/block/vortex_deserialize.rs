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
use databend_common_expression::FilterVisitor;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_expression::types::Bitmap;
use databend_common_expression::visitor::ValueVisitor;
use databend_storages_common_table_meta::meta::ColumnMeta;
use vortex::arrow::IntoArrowArray;
use vortex::iter::ArrayIteratorExt;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::single::SingleThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

use super::parquet::RowSelection;
use crate::io::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;

impl BlockReader {
    /// Deserialize one FUSE block stored as a single Vortex file (see `encode_data_blocks_as_vortex`).
    pub fn deserialize_vortex_chunks(
        &self,
        num_rows: usize,
        _column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
    ) -> Result<DataBlock> {
        let result_rows = selection.map(|s| s.selected_rows).unwrap_or(num_rows);

        if self.projected_schema.fields.is_empty() {
            return Ok(DataBlock::empty_with_rows(result_rows));
        }

        if result_rows == 0 {
            return Ok(DataBlock::empty_with_schema(&self.data_schema()));
        }

        let vortex_bytes = first_raw_vortex_bytes(&column_chunks)?;
        let mut record_batch = decode_vortex_bytes_to_record_batch(&vortex_bytes)?;

        if record_batch.num_rows() != num_rows {
            return Err(ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' row count mismatch: metadata {num_rows}, decoded {}",
                record_batch.num_rows()
            )));
        }

        if let Some(selection) = selection {
            let predicate = bitmap_to_boolean_array(&selection.bitmap)?;
            record_batch = filter_record_batch(&record_batch, &predicate).map_err(|e| {
                ErrorCode::BadBytes(format!(
                    "FUSE storage_format='vortex' failed to apply row filter: {e}"
                ))
            })?;
        }

        let name_paths = column_name_paths(&self.projection, &self.original_schema);

        let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
        for ((i, field), column_node) in self
            .projected_schema
            .fields
            .iter()
            .enumerate()
            .zip(self.project_column_nodes.iter())
        {
            let data_type = field.data_type().into();

            let value = match column_chunks.get(&field.column_id) {
                Some(DataItem::RawData(_)) => {
                    let arrow_array = column_by_name(&record_batch, &name_paths[i]);
                    if column_node.is_nested {
                        return Err(ErrorCode::StorageOther(
                            "FUSE storage_format='vortex' nested projection is not supported yet",
                        ));
                    }
                    Value::from_arrow_rs(arrow_array, &data_type)?
                }
                Some(DataItem::ColumnArray(cached)) => {
                    if column_node.is_nested {
                        return Err(ErrorCode::StorageOther(
                            "unexpected nested field: nested leaf field hits cached (vortex)",
                        ));
                    }
                    let mut value = Value::from_arrow_rs(cached.0.clone(), &data_type)?;
                    if let Some(selection) = selection {
                        let mut filter_visitor = FilterVisitor::new(&selection.bitmap);
                        filter_visitor.visit_value(value)?;
                        value = filter_visitor.take_result().unwrap();
                    }
                    value
                }
                None => Value::Scalar(self.default_vals[i].clone()),
            };
            entries.push(BlockEntry::new(value, || (data_type, result_rows)));
        }
        Ok(DataBlock::new(entries, result_rows))
    }
}

fn first_raw_vortex_bytes(column_chunks: &HashMap<ColumnId, DataItem>) -> Result<Vec<u8>> {
    for (_, item) in column_chunks {
        if let DataItem::RawData(buf) = item {
            return Ok(buf.to_vec());
        }
    }
    Err(ErrorCode::BadBytes(
        "FUSE storage_format='vortex' missing raw column bytes".to_string(),
    ))
}

fn decode_vortex_bytes_to_record_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let rt = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(rt.handle());
    let file = session
        .open_options()
        .open_buffer(bytes.to_vec())
        .map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to open Vortex buffer: {e}"
            ))
        })?;
    let array_ref = file
        .scan()
        .map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to begin scan: {e}"
            ))
        })?
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

fn bitmap_to_boolean_array(bitmap: &Bitmap) -> Result<arrow_array::BooleanArray> {
    let values: Vec<bool> = (0..bitmap.len())
        .map(|i| unsafe { bitmap.get_bit_unchecked(i) })
        .collect();
    Ok(arrow_array::BooleanArray::from(values))
}

fn column_by_name(record_batch: &RecordBatch, names: &[String]) -> ArrayRef {
    let mut array = record_batch.column_by_name(&names[0]).unwrap().clone();
    if names.len() > 1 {
        for name in &names[1..] {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array = struct_array.column_by_name(name).unwrap().clone();
        }
    }
    array
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
