# SEAD JSON API Server (JAS)

This server provides a REST API like `/site/1` which outputs all the information related to that site in an hierarchical JSON format.

This server also acts as the backend for a number of result section charts.

Since a MongoDB is used as a backend storage/cache for the JSON documents, the server can also accept simple queries like below.

To find which site has the analysis_entity 131350:
`/search/datasets.analysis_entities.analysis_entity_id/value/131350`

To find all sites which contain the species 'spelta dicoccum':
`/search/lookup_tables.taxa.species/value/spelta%2Fdicoccum`

## Webclient Search API (New)

The old combined endpoint has been replaced with category-specific endpoints:

`/search/:category/:search`

Where `:category` must be one of:
- `sites`
- `sample_groups`
- `datasets`
- `methods`

Pagination params:
- `limit` (1-50, default 20)
- `page` (default 1)

Example:
- `/search/sites/Glastonbury?limit=20&page=1`
- `/search/sample_groups/phosphate?limit=20&page=1`
- `/search/datasets/phosphate?limit=20&page=1`
- `/search/methods/phosphate?limit=20&page=1`

### Frontend Integration Change

The client should now perform **4 requests per search term** (one per tab/category), instead of one combined request.

Recommended pattern:
1. User types query `q`
2. Fire 4 requests in parallel for page 1:
   - `/search/sites/${q}?limit=20&page=1`
   - `/search/sample_groups/${q}?limit=20&page=1`
   - `/search/datasets/${q}?limit=20&page=1`
   - `/search/methods/${q}?limit=20&page=1`
3. Render each tab independently from its own response
4. On scroll in one tab, only request the next page for that tab/category

### Response Shape (All Categories)

- `query`
- `category` (`key`, `label`)
- `pagination` (`page`, `limit`, `total`, `total_pages`, `has_more`)
- `items` (paged list)

Item fields:
- `site_id`
- `site_name`
- `matched_value`
- `score`
- `is_exact`
- `is_prefix`
- `is_contains`
- `is_fts_only`
- `sample_group_id` (for `sample_groups`, otherwise `null`)
- `dataset_id` (for `datasets`, otherwise `null`)
- `method_id` (for `methods`, otherwise `null`)

