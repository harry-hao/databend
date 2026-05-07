# Vortex Footer Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a process-wide Fuse Vortex footer cache keyed by `location`, stored in `CacheManager`, with count-based eviction (default 1024 entries) and concurrent-miss coalescing so later callers wait for the first footer load.

**Architecture:** Extend `databend-storages-common-cache` with a dedicated `InMemoryLruCache<Footer>` slot and expose it through `CacheManager`, `system.caches`, and `set_cache_capacity`. Then update the Fuse Vortex open path to consult that cache, reuse `with_footer(...)` on hits, and coalesce concurrent misses so only the first caller performs the uncached footer load.

**Tech Stack:** Rust, Databend Fuse storage, `databend-storages-common-cache`, Vortex (`vortex` / `vortex-file` 0.56.0), OpenDAL, Tokio, query integration tests.

---

## File structure (what changes where)

**Modify:**
- `src/query/storages/common/cache/src/caches.rs`
  - Add the `VortexFooterCache` alias
  - Add `CacheValue<Footer>` conversion so `InMemoryLruCache<Footer>` can store entries
- `src/query/storages/common/cache/src/manager.rs`
  - Add the cache slot, getter, constant name, default capacity wiring, `set_cache_capacity` support, and focused unit tests
- `src/query/storages/system/src/caches_table.rs`
  - Surface the new cache in `system.caches`
- `src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs`
  - Add cache lookup / insert logic and concurrent-miss coalescing around `open_vortex_file_async`
- `src/query/service/tests/it/storages/fuse/table_vortex.rs`
  - Add integration coverage for footer-cache population and hit behavior

**Create:**
- None

**Test commands used by this plan:**
- `cargo test -p databend-storages-common-cache test_cache_manager_vortex_footer_cache_defaults -- --nocapture`
- `cargo test -p databend-common-storages-fuse --no-run`
- `cargo test -p databend-query test_fuse_vortex_footer_cache_hits_on_reopen -- --nocapture`
- `cargo test -p databend-query test_fuse_vortex_select_minimal -- --nocapture`

## Task 1: Add the CacheManager slot and cache plumbing

**Files:**
- Modify: `src/query/storages/common/cache/src/caches.rs`
- Modify: `src/query/storages/common/cache/src/manager.rs`
- Modify: `src/query/storages/system/src/caches_table.rs`
- Test: `src/query/storages/common/cache/src/manager.rs`

- [ ] **Step 1: Write the failing manager test for the new cache**

Add this test in `src/query/storages/common/cache/src/manager.rs` under the existing `#[cfg(test)] mod tests`:

```rust
#[test]
fn test_cache_manager_vortex_footer_cache_defaults() -> Result<()> {
    use vortex::file::Footer;

    let max_server_memory_usage = 1024 * 1024;
    let cache_config = CacheConfig {
        enable_table_meta_cache: true,
        ..Default::default()
    };

    let cache_manager = CacheManager::try_new(
        &cache_config,
        &max_server_memory_usage,
        "test_tenant_id",
        false,
    )?;

    let cache = cache_manager
        .get_vortex_footer_cache()
        .expect("vortex footer cache should be initialized");

    assert_eq!(cache.name(), MEMORY_CACHE_VORTEX_FOOTER);
    assert_eq!(cache.items_capacity(), 1024);
    assert_eq!(cache.len(), 0);

    cache_manager.set_cache_capacity(MEMORY_CACHE_VORTEX_FOOTER, 32)?;
    assert_eq!(cache.items_capacity(), 32);

    let _ = std::any::TypeId::of::<Footer>();
    Ok(())
}
```

- [ ] **Step 2: Run the new test to verify it fails**

Run:

```bash
cargo test -p databend-storages-common-cache test_cache_manager_vortex_footer_cache_defaults -- --nocapture
```

Expected: FAIL with compile errors for missing `get_vortex_footer_cache`, missing `MEMORY_CACHE_VORTEX_FOOTER`, and missing `CacheValue<Footer>` conversion.

- [ ] **Step 3: Add the typed cache alias and `CacheValue` conversion**

Update `src/query/storages/common/cache/src/caches.rs` with the new alias and conversion:

```rust
use vortex::file::Footer;

pub type VortexFooterCache = InMemoryLruCache<Footer>;

impl From<Footer> for CacheValue<Footer> {
    fn from(value: Footer) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}
```

Keep the conversion near the other `impl From<T> for CacheValue<T>` blocks.

- [ ] **Step 4: Wire the cache into `CacheManager` with a default capacity of 1024**

Update `src/query/storages/common/cache/src/manager.rs` in four places:

1. Add the slot to the struct:

```rust
vortex_footer_cache: CacheSlot<VortexFooterCache>,
```

2. Create it in `try_new(...)` alongside the other table-meta caches:

```rust
let vortex_footer_cache =
    Self::new_items_cache_slot(MEMORY_CACHE_VORTEX_FOOTER, 1024);
```

3. Store it in both `Self { ... }` construction branches:

```rust
vortex_footer_cache,
```

4. Add the getter, constant, and capacity adjustment branch:

```rust
pub fn get_vortex_footer_cache(&self) -> Option<VortexFooterCache> {
    self.vortex_footer_cache.get()
}

const MEMORY_CACHE_VORTEX_FOOTER: &str = "memory_cache_vortex_footer";
```

And inside `set_cache_capacity(...)`:

```rust
MEMORY_CACHE_VORTEX_FOOTER => {
    Self::set_items_capacity(&self.vortex_footer_cache, new_capacity, name);
}
```

- [ ] **Step 5: Surface the cache in `system.caches`**

Update `src/query/storages/system/src/caches_table.rs` so the new cache shows up with the others:

```rust
let vortex_footer_cache = cache_manager.get_vortex_footer_cache();

if let Some(vortex_footer_cache) = vortex_footer_cache {
    Self::append_row(&vortex_footer_cache, &local_node, &mut columns);
}
```

Place it near the parquet metadata cache block so metadata-oriented caches stay grouped.

- [ ] **Step 6: Run the cache crate test again**

Run:

```bash
cargo test -p databend-storages-common-cache test_cache_manager_vortex_footer_cache_defaults -- --nocapture
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/query/storages/common/cache/src/caches.rs
git add src/query/storages/common/cache/src/manager.rs
git add src/query/storages/system/src/caches_table.rs
git commit -m "$(cat <<'EOF'
feat(cache): add vortex footer cache slot

Wire a dedicated in-memory CacheManager slot for Vortex footers with a default capacity of 1024 items and surface it through system cache introspection.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
EOF
)"
```

## Task 2: Integrate the footer cache into the Vortex open path

**Files:**
- Modify: `src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs`
- Test: `src/query/service/tests/it/storages/fuse/table_vortex.rs`

- [ ] **Step 1: Write the failing integration test for cache hits on reopen**

Add this test to `src/query/service/tests/it/storages/fuse/table_vortex.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vortex_footer_cache_hits_on_reopen() -> anyhow::Result<()> {
    use databend_common_metrics::cache::get_cache_hit_count;
    use databend_common_expression::DataBlock;
    use databend_storages_common_cache::CacheAccessor;
    use databend_storages_common_cache::CacheManager;
    use futures_util::TryStreamExt;

    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    let create = format!(
        "create table {db}.t_vortex_cache(a int, b int) storage_format = 'vortex'"
    );
    let insert = format!(
        "insert into {db}.t_vortex_cache values (1, 10),(2, 20),(3, 30)"
    );
    let select = format!("select sum(b) from {db}.t_vortex_cache where a > 0");

    fixture.execute_command(&create).await?;
    fixture.execute_command(&insert).await?;

    let cache = CacheManager::instance()
        .get_vortex_footer_cache()
        .expect("vortex footer cache should exist");
    cache.clear();
    let cache_name = cache.name().to_string();

    let stream = fixture.execute_query(&select).await?;
    let _blocks = stream.try_collect::<Vec<DataBlock>>().await?;
    assert_eq!(cache.len(), 1, "first read should populate one footer entry");

    let first_hits = get_cache_hit_count(&cache_name);

    let stream = fixture.execute_query(&select).await?;
    let _blocks = stream.try_collect::<Vec<DataBlock>>().await?;
    assert_eq!(cache.len(), 1, "second read should reuse the same footer entry");
    assert!(
        get_cache_hit_count(&cache_name) > first_hits,
        "second read should record a footer-cache hit"
    );

    Ok(())
}
```

- [ ] **Step 2: Run the new integration test to verify it fails**

Run:

```bash
cargo test -p databend-query test_fuse_vortex_footer_cache_hits_on_reopen -- --nocapture
```

Expected: FAIL because the cache stays empty or hit count never increases.

- [ ] **Step 3: Add cache hit / miss helpers around `open_vortex_file_async`**

At the top of `src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs`, add the cache imports and a small in-flight state:

```rust
use std::sync::LazyLock;

use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::Notify;
use vortex::file::Footer;

struct FooterLoadState {
    notify: Notify,
    result: Mutex<Option<Result<Footer>>>,
}

static IN_FLIGHT_VORTEX_FOOTERS: LazyLock<DashMap<String, Arc<FooterLoadState>>> =
    LazyLock::new(DashMap::new);
```

Add helper functions in the same module:

```rust
fn cached_vortex_footer(location: &str) -> Option<Arc<Footer>> {
    CacheManager::instance()
        .get_vortex_footer_cache()
        .and_then(|cache| cache.get(location))
}

fn begin_footer_load(location: &str) -> (Arc<FooterLoadState>, bool) {
    use dashmap::mapref::entry::Entry;

    match IN_FLIGHT_VORTEX_FOOTERS.entry(location.to_string()) {
        Entry::Occupied(entry) => (entry.get().clone(), false),
        Entry::Vacant(entry) => {
            let state = Arc::new(FooterLoadState {
                notify: Notify::new(),
                result: Mutex::new(None),
            });
            entry.insert(state.clone());
            (state, true)
        }
    }
}

fn finish_footer_load(location: &str, state: &Arc<FooterLoadState>, result: Result<Footer>) {
    *state.result.lock() = Some(result);
    IN_FLIGHT_VORTEX_FOOTERS.remove(location);
    state.notify.notify_waiters();
}

async fn wait_for_footer_load(state: Arc<FooterLoadState>) -> Result<Footer> {
    loop {
        if let Some(result) = state.result.lock().clone() {
            return result;
        }
        state.notify.notified().await;
    }
}
```

- [ ] **Step 3.1: Add the missing Fuse crate dependency for `dashmap`**

Update `src/query/storages/fuse/Cargo.toml`:

```toml
dashmap = { workspace = true }
```

Place it with the other third-party dependencies.

- [ ] **Step 4: Change `open_vortex_file_async(...)` to use the cache and coalesce misses**

Replace the current direct open with this two-path structure:

```rust
if let Some(cached_footer) = cached_vortex_footer(location) {
    let read_at = OpendalReadAt::open(operator, location).await.map_err(|e| {
        ErrorCode::BadBytes(format!(
            "FUSE storage_format='vortex' failed to build read_at for {location}: {e}"
        ))
    })?;

    let file = session_for_open
        .open_options()
        .with_initial_read_size(0)
        .with_footer((*cached_footer).clone())
        .open_read_at(read_at)
        .await
        .map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to open cached-footer file via read_at: {e}"
            ))
        })?;

    return Ok(OpenedVortexFile { file, _session: session });
}

let (state, leader) = begin_footer_load(location);
if leader {
    let load_result = async {
        let read_at = OpendalReadAt::open(operator.clone(), location).await.map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to build read_at for {location}: {e}"
            ))
        })?;

        let file = session_for_open
            .open_options()
            .with_initial_read_size(0)
            .open_read_at(read_at)
            .await
            .map_err(|e| {
                ErrorCode::BadBytes(format!(
                    "FUSE storage_format='vortex' failed to open Vortex file via read_at: {e}"
                ))
            })?;

        let footer = file.footer().clone();
        if let Some(cache) = CacheManager::instance().get_vortex_footer_cache() {
            cache.insert(location.to_string(), footer.clone());
        }

        Ok((file, footer))
    }
    .await;

    match load_result {
        Ok((file, footer)) => {
            finish_footer_load(location, &state, Ok(footer));
            Ok(OpenedVortexFile { file, _session: session })
        }
        Err(err) => {
            finish_footer_load(location, &state, Err(err.clone()));
            Err(err)
        }
    }
} else {
    let footer = wait_for_footer_load(state).await?;

    let read_at = OpendalReadAt::open(operator, location).await.map_err(|e| {
        ErrorCode::BadBytes(format!(
            "FUSE storage_format='vortex' failed to build read_at for {location}: {e}"
        ))
    })?;

    let file = session_for_open
        .open_options()
        .with_initial_read_size(0)
        .with_footer(footer)
        .open_read_at(read_at)
        .await
        .map_err(|e| {
            ErrorCode::BadBytes(format!(
                "FUSE storage_format='vortex' failed to open waited-footer file via read_at: {e}"
            ))
        })?;

    Ok(OpenedVortexFile { file, _session: session })
}
```

Keep the existing `metrics_inc_remote_io_read_parts(1);` behavior and preserve the current `ErrorCode::BadBytes` wording style.

- [ ] **Step 5: Run the Fuse compile check and the new integration test**

Run:

```bash
cargo test -p databend-common-storages-fuse --no-run
cargo test -p databend-query test_fuse_vortex_footer_cache_hits_on_reopen -- --nocapture
```

Expected:
- the Fuse crate compiles
- the new integration test passes

- [ ] **Step 6: Commit**

```bash
git add src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs
git add src/query/service/tests/it/storages/fuse/table_vortex.rs
git commit -m "$(cat <<'EOF'
perf(fuse): cache vortex footers by location

Reuse parsed Vortex footers across repeated Fuse block opens and coalesce concurrent footer misses so later callers wait for the first load.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
EOF
)"
```

## Task 3: Final regression checks and cache visibility

**Files:**
- Modify: `src/query/service/tests/it/storages/fuse/table_vortex.rs` (reuse existing tests if small assertion additions are needed)
- Test: `src/query/storages/common/cache/src/manager.rs`

- [ ] **Step 1: Add a small cache-visibility assertion if needed**

If `test_fuse_vortex_footer_cache_hits_on_reopen` needs a stronger sanity check, extend it with:

```rust
let cache = CacheManager::instance()
    .get_vortex_footer_cache()
    .expect("vortex footer cache should exist");
assert_eq!(cache.items_capacity(), 1024);
assert_eq!(cache.name(), "memory_cache_vortex_footer");
```

Only keep these assertions if they make the test clearer without duplicating Task 1.

- [ ] **Step 2: Run the targeted verification set**

Run:

```bash
cargo test -p databend-storages-common-cache test_cache_manager_vortex_footer_cache_defaults -- --nocapture
cargo test -p databend-query test_fuse_vortex_footer_cache_hits_on_reopen -- --nocapture
cargo test -p databend-query test_fuse_vortex_select_minimal -- --nocapture
```

Expected: all three commands PASS.

- [ ] **Step 3: Run one broader Fuse compile check**

Run:

```bash
cargo test -p databend-common-storages-fuse --no-run
```

Expected: PASS with no new compile errors.

- [ ] **Step 4: Commit any final test-only cleanups**

If no extra cleanup was needed, skip this commit. If a small follow-up change was required, use:

```bash
git add src/query/service/tests/it/storages/fuse/table_vortex.rs
git add src/query/storages/common/cache/src/manager.rs
git commit -m "$(cat <<'EOF'
test(fuse): cover vortex footer cache behavior

Add focused cache-manager and query-path regression coverage for the Fuse Vortex footer cache.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
EOF
)"
```

## Self-review checklist (plan quality)

- **Spec coverage:** Task 1 covers `CacheManager` integration, default capacity 1024, and cache visibility. Task 2 covers `location`-keyed footer reuse, `with_footer(...)`, and concurrent-miss waiting. Task 3 covers regression verification.
- **Placeholder scan:** No TODO/TBD placeholders remain; all steps name exact files, code snippets, and commands.
- **Type consistency:** The plan consistently uses `Footer`, `VortexFooterCache`, `MEMORY_CACHE_VORTEX_FOOTER`, and `get_vortex_footer_cache()` across all tasks.
