# Manifest Cache Benchmark Results (WP11)

## Summary

Manifest caching provides large planning wins for small and medium profiles with the current 32MB default. The large profile thrashes at 32MB and 128MB, but a 256MB override yields a 100% hit rate and improves warm-cache planning latency.

## Setup

- Commit: `a0faf9d32712aa9ebb1bafa70ac3b38e52751803`
- Rust: `rustc 1.89.0-nightly (be19eda0d 2025-06-22)`
- Host: `Darwin 25.0.0 arm64` (Mac-7310.lan)
- Commands:
  - Baseline: `cargo bench --bench scan_planning_benchmark -p iceberg`
  - Large Criterion warm-cache (32MB): `cargo bench --bench scan_planning_benchmark -p iceberg -- --sample-size 10 --measurement-time 15 "scan_planning_plan_files_large/warm_cache"`
  - 128MB override: `ICEBERG_MANIFEST_CACHE_MAX_BYTES=134217728 cargo bench --bench scan_planning_benchmark -p iceberg -- --sample-size 10 --measurement-time 10 "scan_planning_plan_files_large/warm_cache"`
  - 160MB override: `ICEBERG_MANIFEST_CACHE_MAX_BYTES=167772160 cargo bench --bench scan_planning_benchmark -p iceberg -- --sample-size 10 --measurement-time 10 "scan_planning_plan_files_large/warm_cache"`
  - 192MB override: `ICEBERG_MANIFEST_CACHE_MAX_BYTES=201326592 cargo bench --bench scan_planning_benchmark -p iceberg -- --sample-size 10 --measurement-time 10 "scan_planning_plan_files_large/warm_cache"`
  - 224MB override: `ICEBERG_MANIFEST_CACHE_MAX_BYTES=234881024 cargo bench --bench scan_planning_benchmark -p iceberg -- --sample-size 10 --measurement-time 10 "scan_planning_plan_files_large/warm_cache"`
  - 256MB override: `ICEBERG_MANIFEST_CACHE_MAX_BYTES=268435456 cargo bench --bench scan_planning_benchmark -p iceberg -- --sample-size 10 --measurement-time 10 "scan_planning_plan_files_large/warm_cache"`
- Cache defaults: enabled, 32MB max, 5min TTL
- Profiles:
  - Small: 10 manifests x 10 entries (100 files), 200 iterations
  - Medium: 100 manifests x 100 entries (10,000 files), 100 iterations
  - Large: 1,000 manifests x 100 entries (100,000 files), 50 iterations
- Scenarios:
  - `disabled_cache`: cache off
  - `cold_cache_invalidate_each_time`: cache on, invalidate every iteration
  - `warm_cache`: cache on, warm once before measurement

## Methodology

Each iteration runs `TableScan::plan_files()` and consumes all tasks. Latencies are recorded per-iteration and p50/p95 are computed by sorting durations. Cache hit/miss/load counters are captured via `CacheMetrics`, and memory footprint via `CacheStats` (weighted size and entry count).

## Results

### Small (10 manifests x 10 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 2995 | 3152 | 0.000 | 0 | 2200 | 2200 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 3046 | 3220 | 0.000 | 0 | 2200 | 2200 | 11 | 168304 | 33554432 |
| warm_cache | 75 | 82 | 1.000 | 2200 | 0 | 0 | 11 | 168304 | 33554432 |

### Medium (100 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 59237 | 60474 | 0.000 | 0 | 10100 | 10100 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 59598 | 60753 | 0.000 | 0 | 10100 | 10100 | 80 | 12010960 | 33554432 |
| warm_cache | 6917 | 7784 | 1.000 | 10100 | 0 | 0 | 101 | 15182464 | 33554432 |

### Large (1,000 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 593840 | 598726 | 0.000 | 0 | 50050 | 50050 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 602573 | 608198 | 0.000 | 0 | 50050 | 50050 | 222 | 33527328 | 33554432 |
| warm_cache | 603209 | 608826 | 0.000 | 0 | 50050 | 50050 | 222 | 33527328 | 33554432 |

Notes:
- The large profile shows zero warm-cache hits with the default 32MB budget, indicating capacity thrash; warm-cache latency is effectively the same as cold/disabled.
- Criterion large warm-cache (32MB, sample-size 10, measurement-time 15s): `time: [607.13 ms 608.30 ms 609.50 ms]`.

## 128MB Override Results

These runs set `ICEBERG_MANIFEST_CACHE_MAX_BYTES=134217728`, which maps to `iceberg.io.manifest.cache.max-total-bytes=134217728` in table properties.

### Small (10 manifests x 10 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 3025 | 3161 | 0.000 | 0 | 2200 | 2200 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 3030 | 3189 | 0.000 | 0 | 2200 | 2200 | 11 | 168304 | 134217728 |
| warm_cache | 73 | 78 | 1.000 | 2200 | 0 | 0 | 11 | 168304 | 134217728 |

### Medium (100 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 59515 | 60148 | 0.000 | 0 | 10100 | 10100 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 60048 | 61221 | 0.000 | 0 | 10100 | 10100 | 86 | 12917104 | 134217728 |
| warm_cache | 6900 | 7619 | 1.000 | 10100 | 0 | 0 | 101 | 15182464 | 134217728 |

### Large (1,000 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 597696 | 611302 | 0.000 | 0 | 50050 | 50050 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 610207 | 616260 | 0.000 | 0 | 50050 | 50050 | 888 | 134109312 | 134217728 |
| warm_cache | 613302 | 648663 | 0.000 | 0 | 50050 | 50050 | 888 | 134109312 | 134217728 |

Notes:
- Large warm-cache remains at 0% hit rate even with 128MB, indicating the working set still exceeds capacity under streaming access.
- Criterion large warm-cache (128MB, sample-size 10, measurement-time 10s): `time: [611.52 ms 614.56 ms 618.61 ms]`.

## 160MB Override Results

These runs set `ICEBERG_MANIFEST_CACHE_MAX_BYTES=167772160`, which maps to `iceberg.io.manifest.cache.max-total-bytes=167772160` in table properties.

### Small (10 manifests x 10 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 3036 | 3173 | 0.000 | 0 | 2200 | 2200 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 3046 | 3197 | 0.000 | 0 | 2200 | 2200 | 6 | 88184 | 167772160 |
| warm_cache | 73 | 76 | 1.000 | 2200 | 0 | 0 | 11 | 168304 | 167772160 |

### Medium (100 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 60314 | 61488 | 0.000 | 0 | 10100 | 10100 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 61010 | 62411 | 0.000 | 0 | 10100 | 10100 | 69 | 10349696 | 167772160 |
| warm_cache | 7337 | 8110 | 1.000 | 10100 | 0 | 0 | 101 | 15182464 | 167772160 |

### Large (1,000 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 616391 | 636660 | 0.000 | 0 | 50050 | 50050 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 625230 | 641331 | 0.000 | 0 | 50050 | 50050 | 993 | 150615872 | 167772160 |
| warm_cache | 80300 | 81253 | 1.000 | 50050 | 0 | 0 | 1001 | 151824064 | 167772160 |

Notes:
- Criterion large warm-cache (160MB, sample-size 10, measurement-time 10s): `time: [79.434 ms 79.556 ms 79.745 ms]`.

## 192MB Override Results

These runs set `ICEBERG_MANIFEST_CACHE_MAX_BYTES=201326592`, which maps to `iceberg.io.manifest.cache.max-total-bytes=201326592` in table properties.

### Small (10 manifests x 10 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 3029 | 3200 | 0.000 | 0 | 2200 | 2200 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 3060 | 3210 | 0.000 | 0 | 2200 | 2200 | 7 | 104208 | 201326592 |
| warm_cache | 75 | 85 | 1.000 | 2200 | 0 | 0 | 11 | 168304 | 201326592 |

### Medium (100 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 61030 | 62554 | 0.000 | 0 | 10100 | 10100 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 61838 | 62785 | 0.000 | 0 | 10100 | 10100 | 98 | 14729392 | 201326592 |
| warm_cache | 7470 | 8813 | 1.000 | 10100 | 0 | 0 | 101 | 15182464 | 201326592 |

### Large (1,000 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 626702 | 652654 | 0.000 | 0 | 50050 | 50050 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 633540 | 648791 | 0.000 | 0 | 50050 | 50050 | 993 | 150615872 | 201326592 |
| warm_cache | 82779 | 86662 | 1.000 | 50050 | 0 | 0 | 1001 | 151824064 | 201326592 |

Notes:
- Criterion large warm-cache (192MB, sample-size 10, measurement-time 10s): `time: [82.800 ms 83.173 ms 83.576 ms]`.

## 224MB Override Results

These runs set `ICEBERG_MANIFEST_CACHE_MAX_BYTES=234881024`, which maps to `iceberg.io.manifest.cache.max-total-bytes=234881024` in table properties.

### Small (10 manifests x 10 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 3052 | 3237 | 0.000 | 0 | 2200 | 2200 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 3105 | 3269 | 0.000 | 0 | 2200 | 2200 | 7 | 104208 | 234881024 |
| warm_cache | 75 | 82 | 1.000 | 2200 | 0 | 0 | 11 | 168304 | 234881024 |

### Medium (100 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 60947 | 62661 | 0.000 | 0 | 10100 | 10100 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 60943 | 62290 | 0.000 | 0 | 10100 | 10100 | 69 | 10349696 | 234881024 |
| warm_cache | 7143 | 8404 | 1.000 | 10100 | 0 | 0 | 101 | 15182464 | 234881024 |

### Large (1,000 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 623436 | 645352 | 0.000 | 0 | 50050 | 50050 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 630443 | 647945 | 0.000 | 0 | 50050 | 50050 | 961 | 145783104 | 234881024 |
| warm_cache | 79867 | 84254 | 1.000 | 50050 | 0 | 0 | 1001 | 151824064 | 234881024 |

Notes:
- Criterion large warm-cache (224MB, sample-size 10, measurement-time 10s): `time: [79.845 ms 80.537 ms 81.034 ms]`.

## 256MB Override Results

These runs set `ICEBERG_MANIFEST_CACHE_MAX_BYTES=268435456`, which maps to `iceberg.io.manifest.cache.max-total-bytes=268435456` in table properties.

### Small (10 manifests x 10 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 3005 | 3156 | 0.000 | 0 | 2200 | 2200 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 3051 | 3261 | 0.000 | 0 | 2200 | 2200 | 6 | 88184 | 268435456 |
| warm_cache | 74 | 80 | 1.000 | 2200 | 0 | 0 | 11 | 168304 | 268435456 |

### Medium (100 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 58897 | 60591 | 0.000 | 0 | 10100 | 10100 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 59791 | 61384 | 0.000 | 0 | 10100 | 10100 | 67 | 10047648 | 268435456 |
| warm_cache | 6865 | 7987 | 1.000 | 10100 | 0 | 0 | 101 | 15182464 | 268435456 |

### Large (1,000 manifests x 100 entries)

| Scenario | p50 (us) | p95 (us) | Hit rate | Hits | Misses | Loads | Entries | Weighted bytes | Max bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| disabled_cache | 596091 | 602860 | 0.000 | 0 | 50050 | 50050 | 0 | 0 | 0 |
| cold_cache_invalidate_each_time | 608813 | 625575 | 0.000 | 0 | 50050 | 50050 | 979 | 148501536 | 268435456 |
| warm_cache | 79016 | 80611 | 1.000 | 50050 | 0 | 0 | 1001 | 151824064 | 268435456 |

Notes:
- Large warm-cache shows a 100% hit rate with 256MB and p50 planning latency drops to ~79ms.
- Criterion large warm-cache (256MB, sample-size 10, measurement-time 10s): `time: [98.071 ms 107.50 ms 111.36 ms]`.

## Invalidation Correctness

The test `test_plan_files_reflects_metadata_update` verifies that after a metadata swap (simulating a new snapshot), `plan_files()` returns a different task count, confirming cached manifests are not reused across metadata updates. Cache invalidation on `with_metadata()` remains intact.

## Recommended Production Defaults

- Keep `iceberg.io.manifest.cache-enabled=true`.
- Increase `iceberg.io.manifest.cache.max-total-bytes` for large tables. The sweep shows a hit-rate knee between 128MB (0%) and 160MB (100%), with p50 warm-cache latency ~79-83ms between 160-224MB and ~79ms at 256MB.
- Keep `iceberg.io.manifest.cache.expiration-interval-ms=300000` (5 min) for general workloads; consider higher TTLs for long-lived coordinators that repeatedly scan the same tables.

## Property Compatibility Note

Iceberg Rust uses different property names and defaults than Iceberg Java:

| Property | Iceberg Java | Iceberg Rust |
| --- | --- | --- |
| Enable cache | `io.manifest.cache-enabled` = false | `iceberg.io.manifest.cache-enabled` = true |
| Max size | `io.manifest.cache.max-total-bytes` = 100MB | `iceberg.io.manifest.cache.max-total-bytes` = 32MB |
| TTL | `io.manifest.cache.expiration-interval-ms` = 60s | `iceberg.io.manifest.cache.expiration-interval-ms` = 300s |

The recommended Rust defaults above are based on the benchmarks in this document.

## Repro Instructions

- `cargo test -p iceberg object_cache`
- `cargo bench --bench scan_planning_benchmark -p iceberg`
