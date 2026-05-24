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
use std::collections::hash_map::Entry;
use std::sync::Arc;

use databend_common_catalog::plan::block_id_in_segment;
use databend_common_catalog::plan::block_idx_in_segment;
use databend_common_catalog::plan::compute_row_id_prefix;
use databend_common_catalog::plan::split_prefix;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::future;
use itertools::Itertools;

use super::fuse_rows_fetcher::RowsFetcher;
use super::parquet_rows_fetcher::RowsFetchMetadataImpl;
use crate::FuseTable;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use databend_common_base::runtime::spawn_blocking;

/// A Vortex-specific implementation of RowFetch.
///
/// It pushes down per-block row indices into Vortex scan via `with_row_indices`,
/// and re-expands duplicates / restores the input row_id order using `final_indices`.
pub(super) struct VortexRowsFetcher {
    snapshot: Option<Arc<TableSnapshot>>,
    table: Arc<FuseTable>,
    reader: Arc<BlockReader>,
    segment_reader: CompactSegmentInfoReader,
    block_meta_lru_cache: InMemoryLruCache<RowsFetchMetadataImpl>,
}

#[async_trait::async_trait]
impl RowsFetcher for VortexRowsFetcher {
    type Metadata = Arc<RowsFetchMetadataImpl>;

    #[async_backtrace::framed]
    async fn initialize(&mut self) -> Result<()> {
        self.snapshot = self.table.read_table_snapshot().await?;
        Ok(())
    }

    async fn fetch_metadata(&mut self, block_id: u64) -> Result<Self::Metadata> {
        if let Some(v) = self.block_meta_lru_cache.get(block_id.to_string()) {
            return Ok(v.clone());
        }

        let (segment, block) = split_prefix(block_id);
        let snapshot = self.snapshot.as_ref().unwrap();

        let (location, ver) = snapshot.segments[segment as usize].clone();
        let segment_load_params = LoadParams {
            ver,
            location,
            len_hint: None,
            put_cache: true,
        };
        let compact_segment_info = self.segment_reader.read(&segment_load_params).await?;

        let blocks = compact_segment_info.block_metas()?;
        let block_idx = block_idx_in_segment(blocks.len(), block as usize);

        static PREFETCH_SIZE: usize = 10;
        let cache_start = block_idx / PREFETCH_SIZE * PREFETCH_SIZE;
        let mut cache_end = block_idx / PREFETCH_SIZE * PREFETCH_SIZE + PREFETCH_SIZE;
        cache_end = std::cmp::min(cache_end, blocks.len());

        let metadata = build_metadata_vortex(&blocks[cache_start..cache_end])?;
        for (block_index, metadata) in (cache_start..cache_end).zip(metadata.into_iter()) {
            let block_id = block_id_in_segment(blocks.len(), block_index);
            let block_id = compute_row_id_prefix(segment, block_id as u64);
            self.block_meta_lru_cache
                .insert(block_id.to_string(), metadata);
        }

        Ok(self
            .block_meta_lru_cache
            .get(block_id.to_string())
            .clone()
            .unwrap())
    }

    #[async_backtrace::framed]
    async fn fetch(
        &mut self,
        row_ids: &[u64],
        metadata: HashMap<u64, Self::Metadata>,
    ) -> Result<DataBlock> {
        let final_block_index = metadata
            .keys()
            .enumerate()
            .map(|(idx, id)| (*id, idx as u32))
            .collect::<HashMap<_, _>>();

        // Per-block original indices (in input order; duplicates preserved).
        let mut tasks_indices: HashMap<u64, Vec<u32>> = HashMap::with_capacity(metadata.len());
        // For each input row_id, record which (block_id, position within tasks_indices[block_id]).
        let mut input_positions: Vec<(u64, u32)> = Vec::with_capacity(row_ids.len());

        for row_id in row_ids {
            let (block_id, idx) = split_row_id(*row_id);
            match tasks_indices.entry(block_id) {
                Entry::Occupied(mut v) => {
                    let task_indices = v.get_mut();
                    let pos = task_indices.len() as u32;
                    task_indices.push(idx as u32);
                    input_positions.push((block_id, pos));
                }
                Entry::Vacant(v) => {
                    v.insert(vec![idx as u32]);
                    input_positions.push((block_id, 0_u32));
                }
            }
        }

        struct BlockPlan {
            uniq_indices: Vec<u32>,
            expanded_positions: Vec<u32>,
        }

        let mut plans: HashMap<u64, BlockPlan> = HashMap::with_capacity(tasks_indices.len());
        for (block_id, orig_indices) in tasks_indices.into_iter() {
            let nums_rows = metadata
                .get(&block_id)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!("missing rowfetch metadata for block {block_id}"))
                })?
                .nums_rows;

            let mut uniq = orig_indices.clone();
            uniq.sort_unstable();
            uniq.dedup();

            if let Some(&max) = uniq.last() {
                if (max as usize) >= nums_rows {
                    return Err(ErrorCode::Internal(format!(
                        "RowID is invalid for Vortex row_indices pushdown, block {block_id}, max_row_idx {max}, block_rows {nums_rows}"
                    )));
                }
            }

            let mut expanded_positions = Vec::with_capacity(orig_indices.len());
            for &idx in orig_indices.iter() {
                let pos = uniq.binary_search(&idx).map_err(|_| {
                    ErrorCode::Internal(
                        "failed to map row index into uniq indices for Vortex".to_string(),
                    )
                })? as u32;
                expanded_positions.push(pos);
            }

            plans.insert(
                block_id,
                BlockPlan {
                    uniq_indices: uniq,
                    expanded_positions,
                },
            );
        }

        // Build final_indices aligned with input order / multiplicity.
        let mut final_indices = Vec::with_capacity(input_positions.len());
        for (block_id, orig_pos) in input_positions.into_iter() {
            let plan = plans.get(&block_id).ok_or_else(|| {
                ErrorCode::Internal(format!("missing rowfetch plan for block {block_id}"))
            })?;
            let row_pos = *plan
                .expanded_positions
                .get(orig_pos as usize)
                .ok_or_else(|| ErrorCode::Internal("rowfetch position out of bounds".to_string()))?;
            final_indices.push((final_block_index[&block_id], row_pos));
        }

        // Fetch each block (uniq_indices pushdown).
        let mut tasks_handle = Vec::with_capacity(plans.len());
        let mut blocks_bytes = 0;
        let mut final_blocks = HashMap::with_capacity(plans.len());
        let mut plans_iter = plans.into_iter().peekable();
        while let Some((block_id, plan)) = plans_iter.next() {
            let metadata = &metadata[&block_id];
            blocks_bytes += metadata.block_bytes;

            let final_take_index = final_block_index[&block_id];
            let join_handle = databend_common_base::runtime::spawn(self.fetch_block(
                metadata.clone(),
                final_take_index,
                plan.uniq_indices,
            ));
            tasks_handle.push(join_handle);

            if blocks_bytes >= 50 * 1024 * 1024 || plans_iter.peek().is_none() {
                let tasks_handle = std::mem::take(&mut tasks_handle);
                let tasks_block = future::try_join_all(tasks_handle).await.unwrap();
                for task_block in tasks_block {
                    let (final_index, block) = task_block?;
                    final_blocks.insert(final_index, block);
                }
            }
        }

        let final_blocks = final_blocks
            .into_iter()
            .sorted_by_key(|(idx, _)| *idx)
            .map(|(_idx, block)| block)
            .collect::<Vec<_>>();

        // Bounds check.
        for (block_idx, row_idx) in final_indices.iter() {
            if *block_idx as usize >= final_blocks.len()
                || *row_idx as usize >= final_blocks[*block_idx as usize].num_rows()
            {
                return Err(ErrorCode::Internal(format!(
                    "RowID is invalid, block idx {block_idx}, row idx {row_idx}, blocks len {}, block idx len {:?}",
                    final_blocks.len(),
                    final_blocks.get(*block_idx as usize).map(|b| b.num_rows()),
                )));
            }
        }

        Ok(DataBlock::take_blocks(&final_blocks, &final_indices))
    }
}

impl VortexRowsFetcher {
    pub(super) fn create(
        table: Arc<FuseTable>,
        reader: Arc<BlockReader>,
        _settings: databend_storages_common_io::ReadSettings,
    ) -> Self {
        let schema = table.schema();
        let operator = table.operator.clone();
        let segment_reader = MetaReaders::segment_info_reader(operator, schema);
        Self {
            table,
            snapshot: None,
            reader,
            segment_reader,
            block_meta_lru_cache: InMemoryLruCache::with_items_capacity(
                String::from("RowFetchBlockMetaCache"),
                128,
            ),
        }
    }

    fn fetch_block(
        &self,
        metadata: Arc<RowsFetchMetadataImpl>,
        final_index: u32,
        mut uniq_indices: Vec<u32>,
    ) -> impl std::future::Future<Output = Result<(u32, DataBlock)>> + use<> {
        let reader = self.reader.clone();
        async move {
            // ScanBuilder requires sorted indices.
            uniq_indices.sort_unstable();
            uniq_indices.dedup();
            let location = metadata.location.clone();

            let block = spawn_blocking(move || {
                reader.deserialize_vortex_chunks_with_scan_filter(
                    &metadata.location,
                    metadata.nums_rows,
                    &metadata.columns_meta,
                    std::collections::HashMap::new(),
                    None,
                    None,
                    None,
                    Some(uniq_indices.as_slice()),
                )
            })
            .await
            .map_err(|e| {
                ErrorCode::Internal(format!(
                    "Vortex row fetch blocking task join failed for {}: {e}",
                    location
                ))
            })??;
            Ok((final_index, block))
        }
    }
}

fn build_metadata_vortex(meta: &[Arc<BlockMeta>]) -> Result<Vec<RowsFetchMetadataImpl>> {
    // For RowFetch we only need enough metadata to locate the block and estimate bytes.
    // We keep logic consistent with ParquetRowsFetcher::build_metadata.
    let mut out = Vec::with_capacity(meta.len());
    for block_meta in meta {
        // For Vortex, `BlockMeta.col_metas` stores per-column meta; RowFetch already projects
        // columns via BlockReader, and Vortex decode currently ignores merge-IO chunks.
        // We still use col stats if present to estimate memory footprint.
        let compression_ratio = if block_meta.file_size == 0 {
            1.0
        } else {
            block_meta.block_size as f64 / block_meta.file_size as f64
        };
        let mut block_bytes = 0;
        let mut average_bytes = 0;
        for (_column_id, column_meta) in &block_meta.col_metas {
            let compressed_size = column_meta.read_bytes(&None);
            let estimate_memory_size = (compressed_size as f64 * compression_ratio) as usize;
            block_bytes += estimate_memory_size;
            if block_meta.row_count > 0 {
                average_bytes += estimate_memory_size / block_meta.row_count as usize;
            }
        }

        out.push(RowsFetchMetadataImpl {
            row_bytes: average_bytes,
            block_bytes,
            nums_rows: block_meta.row_count as usize,
            compression: block_meta.compression,
            location: block_meta.location.0.clone(),
            columns_meta: block_meta.col_metas.clone(),
        });
    }
    Ok(out)
}

