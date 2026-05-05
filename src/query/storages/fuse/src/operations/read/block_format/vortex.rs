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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;

use super::FuseBlockFormat;
use super::ReadBlockMeta;
use crate::io::BlockReadContext;
use crate::operations::read::raw_data_source::RawDataSource;

pub struct FuseVortexBlockFormat;

impl FuseVortexBlockFormat {
    pub fn create() -> Arc<dyn FuseBlockFormat> {
        Arc::new(Self)
    }

    /// Legacy helper for the `open_buffer` fallback: merge-IO must fetch a contiguous prefix that
    /// covers the Vortex footer; use the on-disk block size from segment metadata when available.
    #[allow(dead_code)]
    fn resolve_read_size(
        block_file_size: u64,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
    ) -> u64 {
        if block_file_size > 0 {
            return block_file_size;
        }
        columns_meta
            .values()
            .map(|m| {
                let (o, l) = m.offset_length();
                o.saturating_add(l)
            })
            .max()
            .unwrap_or(0)
    }

    /// Legacy helper for the `open_buffer` fallback: decoding needs the full file bytes; widen each projected column range to the
    /// same `[0, size)` so merge IO coalesces to one remote read while keeping accurate per-column
    /// spans in `FuseBlockPartInfo::columns_meta` for pruning and statistics.
    #[allow(dead_code)]
    fn patch_columns_meta_for_full_file_read(
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        size: u64,
    ) -> HashMap<ColumnId, ColumnMeta> {
        columns_meta
            .iter()
            .map(|(id, meta)| {
                let patched = match meta {
                    ColumnMeta::Vortex(s) => {
                        ColumnMeta::Vortex(SingleColumnMeta::new(0, size, s.num_values))
                    }
                    _ => meta.clone(),
                };
                (*id, patched)
            })
            .collect()
    }

    #[allow(dead_code)]
    pub async fn read_data_by_merge_io_using_full_block_file(
        read_ctx: &BlockReadContext,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
        block_file_size: u64,
    ) -> Result<RawDataSource> {
        let size = Self::resolve_read_size(block_file_size, columns_meta);
        let patched = Self::patch_columns_meta_for_full_file_read(columns_meta, size);
        let source = read_ctx
            .read_columns_data_by_merge_io(settings, location, &patched, ignore_column_ids)
            .await?;

        Ok(RawDataSource::Vortex(source))
    }
}

#[async_trait::async_trait]
impl FuseBlockFormat for FuseVortexBlockFormat {
    #[async_backtrace::framed]
    async fn read_data_by_merge_io(
        &self,
        read_ctx: &BlockReadContext,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<RawDataSource> {
        let source = read_ctx
            .read_columns_data_by_merge_io(settings, location, columns_meta, ignore_column_ids)
            .await?;

        Ok(RawDataSource::Vortex(source))
    }

    async fn read_block_meta(
        &self,
        _operator: &Operator,
        _location: &str,
    ) -> Option<ReadBlockMeta> {
        None
    }
}
