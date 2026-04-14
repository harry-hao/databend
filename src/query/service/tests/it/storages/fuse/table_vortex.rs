//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Integration tests for `ENGINE=FUSE` with `storage_format = 'vortex'`.

use databend_common_catalog::table::Table;
use databend_common_expression::block_debug::assert_blocks_eq;
use databend_common_expression::DataBlock;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_query::sessions::TableContext;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
use futures_util::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vortex_create_insert_minimal() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    let create = format!("create table {db}.t_vortex(a int, b int) storage_format = 'vortex'");
    let insert = format!("insert into {db}.t_vortex values (1, 10),(2, 20),(3, 30)");
    fixture.execute_command(&create).await?;
    fixture.execute_command(&insert).await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(&fixture.default_tenant(), db.as_str(), "t_vortex")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    assert!(
        matches!(fuse_table.get_storage_format(), FuseStorageFormat::Vortex),
        "table option storage_format must remain vortex"
    );
    let snapshot = fuse_table
        .read_table_snapshot()
        .await?
        .expect("snapshot exists after insert");
    assert!(
        snapshot.summary.row_count > 0,
        "expected non-zero snapshot summary row_count for vortex insert"
    );

    let segments_io = SegmentsIO::create(
        ctx.clone(),
        fuse_table.get_operator(),
        fuse_table.schema(),
    );
    let loaded = segments_io
        .read_segments::<SegmentInfo>(&snapshot.segments, false)
        .await?;
    let segment = loaded
        .into_iter()
        .next()
        .expect("at least one segment location")?;
    let block = segment.blocks.first().expect("at least one data block after insert");
    let has_vortex_column = block
        .col_metas
        .values()
        .any(|m| matches!(m, ColumnMeta::Vortex(_)));
    assert!(
        has_vortex_column,
        "persisted block metadata must use ColumnMeta::Vortex (not parquet/native fallback)"
    );

    let full_file_placeholder = block
        .col_metas
        .values()
        .filter(|m| matches!(m, ColumnMeta::Vortex(s) if s.offset == 0 && s.len == block.file_size))
        .count();
    assert_ne!(
        full_file_placeholder,
        block.col_metas.len(),
        "expected per-column Vortex spans from footer/segment_map, not full-file placeholder for every column"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vortex_select_minimal() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    let create = format!("create table {db}.t_vortex_sel(a int) storage_format = 'vortex'");
    let insert = format!("insert into {db}.t_vortex_sel values (1),(2),(3)");
    let select = format!("select count(), sum(a) from {db}.t_vortex_sel");

    fixture.execute_command(&create).await?;
    fixture.execute_command(&insert).await?;

    let stream = fixture.execute_query(&select).await?;
    let blocks = stream.try_collect::<Vec<DataBlock>>().await?;
    assert_eq!(blocks.len(), 1, "expected single aggregation result block");
    // Aggregate column labels may render as generic headers depending on planner path.
    let expected = vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 3        | 6        |",
        "+----------+----------+",
    ];
    assert_blocks_eq(expected, &blocks);

    Ok(())
}
