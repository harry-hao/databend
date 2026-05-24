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

//! Encode Databend `DataBlock`s into an on-disk Vortex file using the Vortex crate surface.
//!
//! Multi-block encode uses Vortex's streaming writer (`blocking().write` over an
//! [`vortex::iter::ArrayIterator`]) so chunks are fed to the layout pipeline without merging all
//! micro-batches into one Arrow `RecordBatch` first.
//!
//! Per-column byte ranges are derived from the Vortex file footer (`segment_map` + root layout),
//! mirroring how `column_parquet_metas` in `operations/util.rs` maps each leaf column id to a
//! physical span inside the on-disk block file.

use std::collections::HashMap;
use std::iter;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use vortex::ArrayRef;
use vortex::VortexSessionDefault;
use vortex::arrow::FromArrowArray;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::SegmentSpec;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::single::SingleThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::iter::ArrayIteratorAdapter;
use vortex::layout::LayoutChildType;
use vortex::layout::LayoutRef;
use vortex::layout::segments::SegmentId;
use vortex::session::VortexSession;

use crate::io::vortex_runtime::vortex_handle;

/// Build [`ColumnMeta::Vortex`] entries from an already-serialized Vortex file.
///
/// Uses the official footer layout tree and `segment_map` only (no Databend-specific headers).
pub fn column_vortex_metas_from_bytes(
    buf: &[u8],
    schema: &TableSchemaRef,
    row_count: u64,
) -> Result<HashMap<ColumnId, ColumnMeta>> {
    let handle = vortex_handle();
    let session = VortexSession::default().with_handle(handle);
    let file = session
        .open_options()
        .open_buffer(buf.to_vec())
        .map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to open buffer for column meta export: {e}"
            ))
        })?;

    let footer = file.footer();
    if footer.row_count() != row_count {
        return Err(ErrorCode::BadBytes(format!(
            "FUSE storage_format='vortex' row count mismatch during column meta export: schema {row_count}, footer {}",
            footer.row_count()
        )));
    }

    let segments = footer.segment_map();
    let root = footer.layout().clone();
    let struct_root = normalize_to_struct_root(root).map_err(|e| {
        ErrorCode::BadBytes(format!(
            "FUSE storage_format='vortex' could not locate struct root layout: {e}"
        ))
    })?;

    let leaf_layouts = collect_leaf_column_layouts(&struct_root).map_err(|e| {
        ErrorCode::BadBytes(format!(
            "FUSE storage_format='vortex' failed to walk layout for column meta export: {e}"
        ))
    })?;

    let column_ids = schema.to_leaf_column_ids();
    if leaf_layouts.len() != column_ids.len() {
        return Err(ErrorCode::BadBytes(format!(
            "FUSE storage_format='vortex' layout/schema mismatch: {} leaf layouts vs {} schema leaf column ids",
            leaf_layouts.len(),
            column_ids.len()
        )));
    }

    let mut col_metas = HashMap::with_capacity(column_ids.len());
    for (idx, column_id) in column_ids.iter().enumerate() {
        let (offset, len) = byte_span_for_layout(&leaf_layouts[idx], segments).map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to compute byte span for column id {column_id}: {e}"
            ))
        })?;
        col_metas.insert(
            *column_id,
            ColumnMeta::Vortex(SingleColumnMeta::new(offset, len, row_count)),
        );
    }

    Ok(col_metas)
}

fn normalize_to_struct_root(mut layout: LayoutRef) -> vortex::error::VortexResult<LayoutRef> {
    loop {
        if layout.dtype().is_struct() {
            return Ok(layout);
        }
        if layout.nchildren() == 0 {
            return Err(vortex::error::vortex_err!(
                "layout has no children before reaching a struct root"
            ));
        }
        layout = layout.child(0)?;
    }
}

/// Depth-first over struct children (skips auxiliary children); each non-struct subtree is one
/// leaf column, aligned with `TableSchema::to_leaf_column_ids()`.
fn collect_leaf_column_layouts(layout: &LayoutRef) -> vortex::error::VortexResult<Vec<LayoutRef>> {
    let mut out = Vec::new();
    collect_leaf_dfs(layout, &mut out)?;
    Ok(out)
}

fn collect_leaf_dfs(
    layout: &LayoutRef,
    out: &mut Vec<LayoutRef>,
) -> vortex::error::VortexResult<()> {
    if layout.dtype().is_struct() {
        for i in 0..layout.nchildren() {
            if matches!(layout.child_type(i), LayoutChildType::Auxiliary(_)) {
                continue;
            }
            collect_leaf_dfs(&layout.child(i)?, out)?;
        }
        Ok(())
    } else {
        out.push(layout.clone());
        Ok(())
    }
}

fn collect_segment_ids_recursive(
    layout: &LayoutRef,
) -> vortex::error::VortexResult<Vec<SegmentId>> {
    let mut ids = layout.segment_ids();
    for i in 0..layout.nchildren() {
        ids.extend(collect_segment_ids_recursive(&layout.child(i)?)?);
    }
    Ok(ids)
}

fn byte_span_for_layout(
    layout: &LayoutRef,
    segments: &[SegmentSpec],
) -> vortex::error::VortexResult<(u64, u64)> {
    let ids = collect_segment_ids_recursive(layout)?;
    if ids.is_empty() {
        return Err(vortex::error::vortex_err!(
            "layout subtree has no segment ids"
        ));
    }
    let mut min_offset = u64::MAX;
    let mut max_end = 0u64;
    for id in ids {
        let spec = segments
            .get(*id as usize)
            .ok_or_else(|| vortex::error::vortex_err!("segment id {} out of range", id))?;
        min_offset = min_offset.min(spec.offset);
        max_end = max_end.max(spec.byte_range().end);
    }
    let len = max_end.saturating_sub(min_offset);
    Ok((min_offset, len))
}

/// Encode one or more `DataBlock`s (same table schema) into a single Vortex file payload.
pub fn encode_data_blocks_as_vortex(
    schema: &TableSchemaRef,
    blocks: &[DataBlock],
) -> Result<(Vec<u8>, HashMap<ColumnId, ColumnMeta>)> {
    if blocks.is_empty() {
        return Err(ErrorCode::BadBytes(
            "FUSE storage_format='vortex' cannot encode an empty block set".to_string(),
        ));
    }

    let row_count: u64 = blocks.iter().map(|b| b.num_rows() as u64).sum();

    let table = schema.as_ref();
    let first_batch = blocks[0].clone().to_record_batch(table)?;
    let first_root = ArrayRef::from_arrow(first_batch, false);
    let dtype = first_root.dtype().clone();

    let schema_arc = schema.clone();
    let blocks_owned: Vec<DataBlock> = blocks.to_vec();
    let chunk_iter = iter::once(Ok(first_root)).chain((1..blocks_owned.len()).map(move |i| {
        let batch = blocks_owned[i]
            .clone()
            .to_record_batch(schema_arc.as_ref())
            .map_err(|e| vortex::error::vortex_err!("{e}"))?;
        Ok(ArrayRef::from_arrow(batch, false))
    }));
    let array_iter = ArrayIteratorAdapter::new(dtype, chunk_iter);

    let rt = SingleThreadRuntime::default();
    let handle = vortex_handle();
    let session = VortexSession::default().with_handle(handle);
    let mut buf = Vec::new();
    session
        .write_options()
        .blocking(&rt)
        .write(&mut buf, array_iter)
        .map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to write Vortex file (streaming chunks): {e}"
            ))
        })?;

    let metas = column_vortex_metas_from_bytes(&buf, schema, row_count)?;

    Ok((buf, metas))
}
