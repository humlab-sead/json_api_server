# Site fetch differential harness

There is no test suite for the `/site` payload and no consumer contract written
down anywhere, so changes to how a site is fetched are verified empirically:
capture the payload before the change, capture it after, and diff the two.

## Scripts

| Script | Purpose |
|---|---|
| `capture-site-baseline.mjs` | Fetches a set of sites from a running server with caching bypassed, writing one JSON file per site plus `_stats.json` (timing, query count, size). |
| `compare-site-baseline.mjs` | Diffs two capture directories structurally and exits non-zero on any difference. |
| `lib/site-diff.mjs` | The diffing logic, importable on its own. |

## Usage

```bash
# Capture the current default (consolidated) implementation
node scripts/capture-site-baseline.mjs --out baseline/after

# Capture the original per-row implementation, still reachable, for comparison
node scripts/capture-site-baseline.mjs --out baseline/before --method true

# Compare them
node scripts/compare-site-baseline.mjs baseline/before baseline/after
```

`--method` selects the implementation via the third path segment of
`/site/:siteId/:noCache?/:alternativeFetchMethod?`:

| value | implementation |
|---|---|
| omitted / `false` | `getSiteConsolidated()` — the default |
| `true` | `getSite()` — the original per-row implementation |
| `postgres` | `getSitePostgres()` — the single-CTE implementation |

Other options: `--base <url>` (default `http://localhost:8485`), `--sites 1,2,79`,
`--timeout <ms>`. For `compare`: `--ordered` to make array order significant,
`--ignore <path>` to drop a field, `--max-groups <n>` to cap the report.

## How the comparison works

The payload is a deep tree of arrays whose ordering is not specified by the
original implementation — most of its queries have no `ORDER BY` — so a textual
diff produces thousands of false positives. The comparison is therefore:

- **order-insensitive by default.** Arrays are compared as multisets. Pass
  `--ordered` to check ordering too, which is how the output was confirmed to be
  deterministic.
- **aligned by identity.** Arrays of records are matched on `analysis_entity_id`,
  `dataset_id`, and similar keys rather than by position. Without this, one side
  carrying an extra column reshuffles both arrays and every neighbouring field is
  reported as changed, burying the real difference.
- **strict about types.** The `pg` driver returns `numeric` and `bigint` as
  strings and `timestamptz` as a `Date`. A `numeric` silently becoming the number
  `12.5` instead of the string `"12.5"` is reported, not hidden. This is what
  caught the type drift in the single-CTE implementation.
- **aggregated by path pattern.** A systematic change appears as one line with a
  count rather than thousands of entries.

`api_source` and `server_version` are always ignored.

## Query counting

Set `JAS_QUERY_STATS=true` to enable `/debug/query-stats`, which reports the SQL
round-trip counter and the pg pool counters. `capture-site-baseline.mjs` reads it
before and after each site to attribute a query count to that request, and it is
how the before/after numbers in the refactor were measured. The endpoint and the
counter are absent unless the flag is set.

## Site set

The default set covers every data fetching module that can fire, every size
class, and the known edge cases (no sample groups, nonexistent id, the only site
with analysis entities carrying two relative dates). See `DEFAULT_SITES` in
`capture-site-baseline.mjs`, where each entry says why it is there.
