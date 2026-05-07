# Fuse Vortex footer cache design

Date: 2026-05-07  
Status: Draft (for review)  
Audience: Databend storage / Fuse / Vortex integration

## Summary

This spec proposes a process-wide footer cache for Fuse Vortex block reads so Databend can reuse
parsed Vortex `Footer` metadata across repeated opens of the same block file. The cache is bounded
by a fixed default capacity of **1024 cached footers**.

The cache stores:

- **key**: `location`
- **value**: `Footer`

On cache hit, Databend will open the Vortex file with:

- `session.open_options().with_footer(cached_footer.clone()).open_read_at(read_at)`

This avoids repeated footer reads and footer deserialization while keeping the rest of the scan path
unchanged.

## Problem statement

Databend's current Fuse Vortex read path opens a block file with `open_read_at(...)` each time the
file is needed. Even after the recent single-open reuse for prewhere/remain within a single
`ReadState`, later opens of the same `part.location` still re-read and re-parse the footer.

For Vortex files, the footer contains the layout tree, segment map, row count, dtype, and optional
statistics needed to initialize the file reader. Repeating that work is wasteful when the same
block file is reopened in the process.

## Goals / Non-goals

### Goals

- **G1**: Reuse a previously parsed `Footer` for the same Fuse Vortex block `location`.
- **G2**: Integrate with Databend's existing cache conventions (`CacheManager`,
  `InMemoryLruCache`).
- **G3**: Use a simple count-based eviction policy with a default capacity of 1024 entries.
- **G4**: Avoid duplicate concurrent footer loads for the same `location`.
- **G5**: Keep the change local to the Vortex open path; scanning and decoding behavior should stay
  the same.

### Non-goals

- **NG1**: Do not cache `VortexFile`, `OpenedVortexFile`, or any object with active read/session
  state.
- **NG2**: Do not introduce active invalidation or mutation tracking for Fuse block files.
- **NG3**: Do not change block pruning, projection planning, or scan semantics.
- **NG4**: Do not require footer-size accounting or per-entry byte estimation.

## Background and constraints

### Fuse file immutability

For Fuse block files, `location` is treated as the identity of an immutable object. New data is
written as new files and referenced by new snapshot / segment metadata rather than appended in
place. Therefore, using `location` as the footer-cache key is acceptable for this design.

### Existing Databend cache conventions

Databend's storage caches are centrally managed via `CacheManager`, while individual caches are
implemented using typed wrappers such as `InMemoryLruCache<T>`. Those caches use `String` keys and
can be configured either by item count or by bytes.

## Alternatives considered

### A1. Cache `Footer` only (**recommended**)

- Reuse the parsed footer via `with_footer(...)`
- Low lifecycle complexity
- No session / reader reuse problems
- Matches the exact optimization target: repeated footer read + parse

### A2. Cache `OpenedVortexFile` / `VortexFile`

- Saves more work on hit
- Introduces stateful object reuse across callers
- Couples cache entries to session, segment source, runtime, and drop order
- Rejected as too complex for the benefit

### A3. Query-local or reader-local footer cache

- Simpler lifetime
- Misses most of the benefit because it does not reuse across repeated opens in the process
- Rejected because the desired key is global `location`

## Proposed design

### Cache entry

Store the reusable Vortex `Footer` directly as the cached value.

No wrapper struct is required for the first version because the cache will evict by item count
rather than by estimated bytes.

### Cache location

Add a dedicated cache slot to `CacheManager`, for example:

- `vortex_footer_cache: CacheSlot<VortexFooterCache>`

Where:

- `type VortexFooterCache = InMemoryLruCache<Footer>;`

This keeps the new cache aligned with Databend's existing storage-cache structure and exposes a
clear getter similar to other cache types.

### Cache key

Use the Fuse Vortex block `location` string directly as the cache key.

This matches:

- the user-approved design decision
- existing Databend cache patterns such as parquet metadata
- the immutability model of Fuse block files

### Cache capacity

Use an item-count cache with a default capacity of **1024** entries.

Rationale:

- avoids introducing custom size-accounting logic for `Footer`
- matches the user-approved simplification for this design
- keeps the first implementation focused on correctness and reuse behavior

If realistic workloads later show that item-count eviction is insufficient, byte-based accounting can
be introduced in a follow-up change.

### Concurrency behavior

Concurrent misses for the same `location` should be coalesced so later callers wait for the first
load instead of loading the same footer repeatedly.

Use a lightweight in-flight map keyed by `location`, conceptually:

- if cache hit: use it immediately
- if cache miss and no in-flight loader exists: register loader and perform open
- if cache miss and loader already exists: wait for the first loader to publish the result

The coalescing mechanism only needs to deduplicate footer acquisition. It does not need to reuse the
entire `VortexFile`.

### Open path integration

Modify `open_vortex_file_async(...)` as the single integration point:

1. Create / obtain the `read_at` source as today.
2. Look up `location` in the global footer cache.
3. On hit:
   - call `open_options().with_footer(cached_footer.clone()).open_read_at(read_at)`
4. On miss:
   - call the existing open path without `with_footer(...)`
   - extract `file.footer().clone()`
   - insert the new `Footer`
5. Return the opened file wrapper as today.

The scan and decode path remains unchanged.

## Data flow

### Cache hit

1. Caller requests `open_vortex_file_async(operator, location)`
2. `read_at` is created
3. footer cache returns `Footer`
4. open uses `with_footer(cached_footer.clone())`
5. Vortex open skips footer read/parse work
6. scan proceeds normally

### Cache miss

1. Caller requests `open_vortex_file_async(operator, location)`
2. in-flight dedup decides whether this caller loads or waits
3. loader opens file through the normal path
4. loader clones `file.footer()`
5. loader inserts cache entry
6. waiters resume and use the cached footer on later opens

## Error handling

- Failed opens must **not** populate the cache.
- Failed loads must wake waiters and propagate the same error; waiters must not hang.
- If an in-flight load fails, the in-flight marker must be cleaned up so later attempts can retry.

## Testing & validation

### Integration tests

- Add a Vortex-focused test showing that repeated opens of the same `location` populate and reuse
  the footer cache.
- Add a test for count-based cache insertion and eviction at the configured capacity.
- Add a test ensuring concurrent misses for the same `location` are coalesced rather than loaded
  multiple times.

### Regression safety

- Existing Vortex query tests should continue to pass unchanged.
- Existing prewhere single-open behavior should remain intact; this cache is additive and
  cross-open, not a replacement for per-`ReadState` reuse.

## Rollout notes

- This cache should start as an internal Vortex/Fuse optimization only.
- Capacity can be tuned independently after observing realistic workloads.
