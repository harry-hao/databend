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

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_metrics::storage::*;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::read_data_source::ReadDataSource;
use super::read_state::ReadState;
use super::util::add_data_block_meta;
use super::util::need_reserve_block_info;
use super::vortex_data_source::VortexDataSource;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::referenced_field_names;
use crate::io::translate_expr_to_vortex;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub struct VortexDeserializeDataTransform {
    // Kept for parity with parquet/native deserializers (prewhere / runtime filters evolution).
    #[allow(dead_code)]
    ctx: Arc<dyn TableContext>,
    #[allow(dead_code)]
    scan_id: usize,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,
    index_reader: Arc<Option<AggIndexReader>>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    src_schema: DataSchema,
    output_schema: DataSchema,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<VortexDataSource>,

    base_block_ids: Option<Scalar>,
    need_reserve_block_info: bool,

    read_state: Option<ReadState>,
}

unsafe impl Send for VortexDeserializeDataTransform {}

impl VortexDeserializeDataTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        index_reader: Arc<Option<AggIndexReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();

        let src_schema: DataSchema = (block_reader.schema().as_ref()).into();

        let mut output_schema = plan.schema().as_ref().clone();
        output_schema.remove_internal_fields();
        let output_schema: DataSchema = (&output_schema).into();

        let prewhere_info = plan
            .push_downs
            .as_ref()
            .and_then(|p| p.prewhere.as_ref())
            .cloned();

        let read_state = if prewhere_info.is_some()
            || !ctx.get_runtime_filters(plan.scan_id).is_empty()
        {
            Some(ReadState::create(
                ctx.clone(),
                plan.scan_id,
                prewhere_info.as_ref(),
                block_reader.as_ref(),
            )?)
        } else {
            None
        };

        let (need_reserve_block_info, _) = need_reserve_block_info(ctx.clone(), plan.table_index);
        Ok(ProcessorPtr::create(Box::new(VortexDeserializeDataTransform {
            ctx: ctx.clone(),
            scan_id: plan.scan_id,
            scan_progress,
            block_reader,
            index_reader,
            input,
            output,
            output_data: None,
            src_schema,
            output_schema,
            parts: vec![],
            chunks: vec![],
            base_block_ids: plan.base_block_ids.clone(),
            need_reserve_block_info,
            read_state,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for VortexDeserializeDataTransform {
    fn name(&self) -> String {
        String::from("VortexDeserializeDataTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(source_meta) = data_block.take_meta() {
                if let Some(source_meta) =
                    DataSourceWithMeta::<ReadDataSource>::downcast_from(source_meta)
                {
                    self.parts = source_meta.meta;
                    self.chunks = source_meta
                        .data
                        .into_iter()
                        .map(ReadDataSource::into_vortex)
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let part = self.parts.pop();
        let chunks = self.chunks.pop();
        if let Some((part, read_res)) = part.zip(chunks) {
            match read_res {
                VortexDataSource::AggIndex((actual_part, data)) => {
                    let agg_index_reader = self.index_reader.as_ref().as_ref().unwrap();
                    let block = agg_index_reader.deserialize_parquet_data(actual_part, data)?;

                    let progress_values = ProgressValues {
                        rows: block.num_rows(),
                        bytes: block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ScanBytes,
                        block.memory_size(),
                    );

                    self.output_data = Some(block);
                }
                VortexDataSource::Normal(_part) => {
                    let start = Instant::now();
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;

                    let mut data_block = match &self.read_state {
                        Some(read_state) => match (&read_state.filters, read_state.runtime_filters.is_empty()) {
                            (Some(filter), true) => {
                                match translate_expr_to_vortex(filter, &read_state.prewhere_schema)? {
                                    Some(vortex_filter) => {
                                        let extra = referenced_field_names(filter, &read_state.prewhere_schema);
                                        self.block_reader.deserialize_vortex_chunks_with_scan_filter(
                                            &fuse_part.location,
                                            fuse_part.nums_rows,
                                            &fuse_part.columns_meta,
                                            std::collections::HashMap::new(),
                                            None,
                                            Some(vortex_filter),
                                            extra,
                                        )?
                                    }
                                    None => {
                                        let (data_block, _row_selection, _bitmap) =
                                            read_state.deserialize_and_filter_vortex(&fuse_part)?;
                                        data_block
                                    }
                                }
                            }
                            _ => {
                                let (data_block, _row_selection, _bitmap) =
                                    read_state.deserialize_and_filter_vortex(&fuse_part)?;
                                data_block
                            }
                        },
                        None => self.block_reader.deserialize_vortex_chunks(
                            &fuse_part.location,
                            fuse_part.nums_rows,
                            &fuse_part.columns_meta,
                            std::collections::HashMap::new(),
                            None,
                        )?,
                    };

                    metrics_inc_remote_io_deserialize_milliseconds(
                        start.elapsed().as_millis() as u64
                    );

                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ScanBytes,
                        data_block.memory_size(),
                    );

                    data_block = data_block.resort(&self.src_schema, &self.output_schema)?;

                    let offsets = None;

                    data_block = add_data_block_meta(
                        data_block,
                        &fuse_part,
                        offsets,
                        self.base_block_ids.clone(),
                        self.block_reader.update_stream_columns(),
                        self.block_reader.query_internal_columns(),
                        self.need_reserve_block_info,
                    )?;

                    self.output_data = Some(data_block);
                }
            }
        }

        Ok(())
    }
}
