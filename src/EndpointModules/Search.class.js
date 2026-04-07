class Search {
    constructor(app) {
        this.app = app;

        this.setupEndpoints();
    }

    setupEndpoints() {
        this.app.expressApp.get('/search/:category/:search', async (req, res) => {
            const category = req.params.category;
            const categoryMeta = this.resolveSearchCategory(category);
            if(!categoryMeta) {
                res.status(400);
                res.send(JSON.stringify({
                    error: "Invalid category. Must be one of: sites, sample_groups, datasets, methods"
                }, null, 2));
                return;
            }

            let decodedQuery = req.params.search;
            try {
                decodedQuery = decodeURIComponent(req.params.search);
            }
            catch(error) {
                // Keep original query string if decode fails
            }
            const searchTerm = this.normalizeGeneralSearchTerm(decodedQuery);

            if(searchTerm.length < 2) {
                res.status(400);
                res.send(JSON.stringify({ error: "Search term must be at least 2 characters long" }, null, 2));
                return;
            }

            const requestedLimit = Number.parseInt(req.query.limit, 10);
            const perCategoryLimit = Number.isInteger(requestedLimit)
                ? Math.max(1, Math.min(requestedLimit, 50))
                : 20;
            const requestedPage = Number.parseInt(req.query.page, 10);
            const page = Number.isInteger(requestedPage)
                ? Math.max(1, requestedPage)
                : 1;

            try {
                const searchResults = await this.categorySearchPostgres(categoryMeta, searchTerm, page, perCategoryLimit);
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(searchResults, null, 2));
            }
            catch(error) {
                console.error("Category Postgres search failed");
                console.error(error);
                res.status(500).send(JSON.stringify({ error: "Internal server error" }, null, 2));
            }
        });
    }

    normalizeGeneralSearchTerm(searchTerm) {
        if(typeof searchTerm != "string") {
            return "";
        }

        // Remove control characters and collapse repeated whitespace.
        const cleaned = searchTerm
            .replace(/[\u0000-\u001F\u007F]/g, " ")
            .replace(/\s+/g, " ")
            .trim();

        if(!/[\p{L}\p{N}]/u.test(cleaned)) {
            return "";
        }

        return cleaned;
    }

    resolveSearchCategory(category) {
        const categoryLookup = {
            sites: {
                key: "sites",
                label: "Site Name"
            },
            sample_groups: {
                key: "sample_groups",
                label: "Sample Group"
            },
            datasets: {
                key: "datasets",
                label: "Dataset"
            },
            methods: {
                key: "methods",
                label: "Method"
            }
        };

        if(typeof categoryLookup[category] == "undefined") {
            return false;
        }
        return categoryLookup[category];
    }

    buildCategorySearchResponse(searchTerm, categoryMeta, page, perCategoryLimit, rows) {
        let totalCount = 0;
        const items = [];

        rows.forEach(row => {
            totalCount = Number.parseInt(row.total_count, 10) || totalCount;
            if(row.site_id === null || typeof row.site_id == "undefined") {
                return;
            }

            const siteId = Number.parseInt(row.site_id, 10);
            const sampleGroupId = (row.sample_group_id === null || typeof row.sample_group_id == "undefined")
                ? null
                : Number.parseInt(row.sample_group_id, 10);
            const datasetId = (row.dataset_id === null || typeof row.dataset_id == "undefined")
                ? null
                : Number.parseInt(row.dataset_id, 10);
            const methodId = (row.method_id === null || typeof row.method_id == "undefined")
                ? null
                : Number.parseInt(row.method_id, 10);

            items.push({
                site_id: Number.isInteger(siteId) ? siteId : null,
                site_name: row.site_name,
                score: Number((Number.parseFloat(row.score) || 0).toFixed(6)),
                is_exact: row.is_exact === true,
                is_prefix: row.is_prefix === true,
                is_contains: row.is_contains === true,
                is_fts_only: row.is_fts_only === true,
                matched_value: row.matched_value,
                sample_group_id: Number.isInteger(sampleGroupId) ? sampleGroupId : null,
                dataset_id: Number.isInteger(datasetId) ? datasetId : null,
                method_id: Number.isInteger(methodId) ? methodId : null
            });
        });

        const totalPages = totalCount > 0
            ? Math.ceil(totalCount / perCategoryLimit)
            : 0;

        return {
            query: searchTerm,
            algorithm: "postgres_category_search_v2",
            category: categoryMeta,
            pagination: {
                page,
                limit: perCategoryLimit,
                total: totalCount,
                total_pages: totalPages,
                has_more: page < totalPages
            },
            items
        };
    }

    async categorySearchPostgres(categoryMeta, searchTerm, page = 1, perCategoryLimit = 20) {
        const pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            throw new Error("Could not acquire Postgres connection");
        }

        const offset = (page - 1) * perCategoryLimit;
        const upperBound = offset + perCategoryLimit;

        let sql = "";

        if(categoryMeta.key == "sites") {
            sql = `
            WITH params AS (
                SELECT
                    trim($1)::text AS raw_query,
                    websearch_to_tsquery('simple', trim($1)) AS tsq
            ),
            hits_raw AS (
                SELECT
                    s.site_id,
                    coalesce(s.site_name, '') AS site_name,
                    coalesce(s.site_name, '') AS matched_value,
                    (
                        CASE WHEN lower(s.site_name) = lower(p.raw_query) THEN 6.0 ELSE 0 END
                        + CASE WHEN lower(s.site_name) LIKE lower(p.raw_query) || '%' THEN 3.0 ELSE 0 END
                        + CASE WHEN s.site_name ILIKE '%' || p.raw_query || '%' THEN 1.5 ELSE 0 END
                        + ts_rank_cd(setweight(to_tsvector('simple', coalesce(s.site_name, '')), 'A'), p.tsq, 32)
                    ) AS score,
                    (lower(s.site_name) = lower(p.raw_query)) AS is_exact,
                    (
                        lower(s.site_name) LIKE lower(p.raw_query) || '%'
                        AND lower(s.site_name) <> lower(p.raw_query)
                    ) AS is_prefix,
                    (
                        s.site_name ILIKE '%' || p.raw_query || '%'
                        AND NOT (lower(s.site_name) LIKE lower(p.raw_query) || '%')
                    ) AS is_contains,
                    (
                        to_tsvector('simple', s.site_name) @@ p.tsq
                        AND NOT (s.site_name ILIKE '%' || p.raw_query || '%')
                    ) AS is_fts_only,
                    NULL::integer AS sample_group_id,
                    NULL::integer AS dataset_id,
                    NULL::integer AS method_id
                FROM tbl_sites s
                CROSS JOIN params p
                WHERE s.site_name IS NOT NULL
                    AND (
                        to_tsvector('simple', s.site_name) @@ p.tsq
                        OR s.site_name ILIKE '%' || p.raw_query || '%'
                    )
            ),
            hits AS (
                SELECT *
                FROM hits_raw
                WHERE score > 0
            ),
            total_hits AS (
                SELECT count(*)::int AS total_count
                FROM hits
            ),
            ranked AS (
                SELECT
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id,
                    row_number() OVER (
                        ORDER BY score DESC, site_name ASC
                    ) AS rn
                FROM hits
            ),
            paged_hits AS (
                SELECT
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id
                FROM ranked
                WHERE rn > $2
                    AND rn <= $3
            )
            SELECT
                t.total_count,
                ph.site_id,
                ph.site_name,
                ph.matched_value,
                ph.score,
                ph.is_exact,
                ph.is_prefix,
                ph.is_contains,
                ph.is_fts_only,
                ph.sample_group_id,
                ph.dataset_id,
                ph.method_id
            FROM total_hits t
            LEFT JOIN paged_hits ph ON true
            ORDER BY
                CASE WHEN ph.site_id IS NULL THEN 1 ELSE 0 END ASC,
                ph.score DESC,
                ph.site_name ASC
            `;
        }
        else if(categoryMeta.key == "sample_groups") {
            sql = `
            WITH params AS (
                SELECT
                    trim($1)::text AS raw_query,
                    websearch_to_tsquery('simple', trim($1)) AS tsq
            ),
            hits_raw AS (
                SELECT
                    s.site_id,
                    coalesce(s.site_name, '') AS site_name,
                    trim(concat_ws(' ', coalesce(sg.sample_group_name, ''), coalesce(sg.sample_group_description, ''))) AS matched_value,
                    (
                        CASE WHEN lower(coalesce(sg.sample_group_name, '')) = lower(p.raw_query) THEN 4.5 ELSE 0 END
                        + CASE WHEN lower(coalesce(sg.sample_group_name, '')) LIKE lower(p.raw_query) || '%' THEN 2.0 ELSE 0 END
                        + CASE WHEN coalesce(sg.sample_group_name, '') ILIKE '%' || p.raw_query || '%' THEN 1.2 ELSE 0 END
                        + CASE WHEN coalesce(sg.sample_group_description, '') ILIKE '%' || p.raw_query || '%' THEN 0.6 ELSE 0 END
                        + ts_rank_cd(setweight(to_tsvector('simple', coalesce(sg.sample_group_name, '')), 'B'), p.tsq, 32)
                        + ts_rank_cd(setweight(to_tsvector('simple', coalesce(sg.sample_group_description, '')), 'C'), p.tsq, 32)
                    ) AS score,
                    (lower(coalesce(sg.sample_group_name, '')) = lower(p.raw_query)) AS is_exact,
                    (
                        lower(coalesce(sg.sample_group_name, '')) LIKE lower(p.raw_query) || '%'
                        AND lower(coalesce(sg.sample_group_name, '')) <> lower(p.raw_query)
                    ) AS is_prefix,
                    (
                        (
                            coalesce(sg.sample_group_name, '') ILIKE '%' || p.raw_query || '%'
                            OR coalesce(sg.sample_group_description, '') ILIKE '%' || p.raw_query || '%'
                        )
                        AND NOT (lower(coalesce(sg.sample_group_name, '')) LIKE lower(p.raw_query) || '%')
                    ) AS is_contains,
                    (
                        (
                            to_tsvector('simple', coalesce(sg.sample_group_name, '')) @@ p.tsq
                            OR to_tsvector('simple', coalesce(sg.sample_group_description, '')) @@ p.tsq
                        )
                        AND NOT (
                            coalesce(sg.sample_group_name, '') ILIKE '%' || p.raw_query || '%'
                            OR coalesce(sg.sample_group_description, '') ILIKE '%' || p.raw_query || '%'
                        )
                    ) AS is_fts_only,
                    sg.sample_group_id,
                    NULL::integer AS dataset_id,
                    NULL::integer AS method_id
                FROM tbl_sample_groups sg
                JOIN tbl_sites s ON s.site_id = sg.site_id
                CROSS JOIN params p
                WHERE
                    (
                        to_tsvector('simple', coalesce(sg.sample_group_name, '')) @@ p.tsq
                        OR to_tsvector('simple', coalesce(sg.sample_group_description, '')) @@ p.tsq
                        OR coalesce(sg.sample_group_name, '') ILIKE '%' || p.raw_query || '%'
                        OR coalesce(sg.sample_group_description, '') ILIKE '%' || p.raw_query || '%'
                    )
            ),
            hits AS (
                SELECT DISTINCT ON (site_id)
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id
                FROM hits_raw
                WHERE score > 0
                ORDER BY site_id, score DESC, matched_value ASC
            ),
            total_hits AS (
                SELECT count(*)::int AS total_count
                FROM hits
            ),
            ranked AS (
                SELECT
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id,
                    row_number() OVER (
                        ORDER BY score DESC, site_name ASC
                    ) AS rn
                FROM hits
            ),
            paged_hits AS (
                SELECT
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id
                FROM ranked
                WHERE rn > $2
                    AND rn <= $3
            )
            SELECT
                t.total_count,
                ph.site_id,
                ph.site_name,
                ph.matched_value,
                ph.score,
                ph.is_exact,
                ph.is_prefix,
                ph.is_contains,
                ph.is_fts_only,
                ph.sample_group_id,
                ph.dataset_id,
                ph.method_id
            FROM total_hits t
            LEFT JOIN paged_hits ph ON true
            ORDER BY
                CASE WHEN ph.site_id IS NULL THEN 1 ELSE 0 END ASC,
                ph.score DESC,
                ph.site_name ASC
            `;
        }
        else if(categoryMeta.key == "datasets") {
            sql = `
            WITH params AS (
                SELECT
                    trim($1)::text AS raw_query,
                    websearch_to_tsquery('simple', trim($1)) AS tsq
            ),
            site_dataset_links AS (
                SELECT
                    sg.site_id,
                    d.dataset_id,
                    d.dataset_name
                FROM tbl_sample_groups sg
                JOIN tbl_physical_samples ps ON ps.sample_group_id = sg.sample_group_id
                JOIN tbl_analysis_entities ae ON ae.physical_sample_id = ps.physical_sample_id
                JOIN tbl_datasets d ON d.dataset_id = ae.dataset_id
                GROUP BY sg.site_id, d.dataset_id, d.dataset_name
            ),
            hits_raw AS (
                SELECT
                    sdl.site_id,
                    coalesce(s.site_name, '') AS site_name,
                    coalesce(sdl.dataset_name, '') AS matched_value,
                    (
                        CASE WHEN lower(coalesce(sdl.dataset_name, '')) = lower(p.raw_query) THEN 4.5 ELSE 0 END
                        + CASE WHEN lower(coalesce(sdl.dataset_name, '')) LIKE lower(p.raw_query) || '%' THEN 2.0 ELSE 0 END
                        + CASE WHEN coalesce(sdl.dataset_name, '') ILIKE '%' || p.raw_query || '%' THEN 1.2 ELSE 0 END
                        + ts_rank_cd(setweight(to_tsvector('simple', coalesce(sdl.dataset_name, '')), 'B'), p.tsq, 32)
                    ) AS score,
                    (lower(coalesce(sdl.dataset_name, '')) = lower(p.raw_query)) AS is_exact,
                    (
                        lower(coalesce(sdl.dataset_name, '')) LIKE lower(p.raw_query) || '%'
                        AND lower(coalesce(sdl.dataset_name, '')) <> lower(p.raw_query)
                    ) AS is_prefix,
                    (
                        coalesce(sdl.dataset_name, '') ILIKE '%' || p.raw_query || '%'
                        AND NOT (lower(coalesce(sdl.dataset_name, '')) LIKE lower(p.raw_query) || '%')
                    ) AS is_contains,
                    (
                        to_tsvector('simple', coalesce(sdl.dataset_name, '')) @@ p.tsq
                        AND NOT (coalesce(sdl.dataset_name, '') ILIKE '%' || p.raw_query || '%')
                    ) AS is_fts_only,
                    NULL::integer AS sample_group_id,
                    sdl.dataset_id,
                    NULL::integer AS method_id
                FROM site_dataset_links sdl
                JOIN tbl_sites s ON s.site_id = sdl.site_id
                CROSS JOIN params p
                WHERE sdl.dataset_name IS NOT NULL
                    AND (
                        to_tsvector('simple', sdl.dataset_name) @@ p.tsq
                        OR sdl.dataset_name ILIKE '%' || p.raw_query || '%'
                    )
            ),
            hits AS (
                SELECT DISTINCT ON (site_id)
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id
                FROM hits_raw
                WHERE score > 0
                ORDER BY site_id, score DESC, matched_value ASC
            ),
            total_hits AS (
                SELECT count(*)::int AS total_count
                FROM hits
            ),
            ranked AS (
                SELECT
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id,
                    row_number() OVER (
                        ORDER BY score DESC, site_name ASC
                    ) AS rn
                FROM hits
            ),
            paged_hits AS (
                SELECT
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id
                FROM ranked
                WHERE rn > $2
                    AND rn <= $3
            )
            SELECT
                t.total_count,
                ph.site_id,
                ph.site_name,
                ph.matched_value,
                ph.score,
                ph.is_exact,
                ph.is_prefix,
                ph.is_contains,
                ph.is_fts_only,
                ph.sample_group_id,
                ph.dataset_id,
                ph.method_id
            FROM total_hits t
            LEFT JOIN paged_hits ph ON true
            ORDER BY
                CASE WHEN ph.site_id IS NULL THEN 1 ELSE 0 END ASC,
                ph.score DESC,
                ph.site_name ASC
            `;
        }
        else if(categoryMeta.key == "methods") {
            sql = `
            WITH params AS (
                SELECT
                    trim($1)::text AS raw_query,
                    websearch_to_tsquery('simple', trim($1)) AS tsq
            ),
            site_dataset_method_links AS (
                SELECT
                    sg.site_id,
                    d.method_id
                FROM tbl_sample_groups sg
                JOIN tbl_physical_samples ps ON ps.sample_group_id = sg.sample_group_id
                JOIN tbl_analysis_entities ae ON ae.physical_sample_id = ps.physical_sample_id
                JOIN tbl_datasets d ON d.dataset_id = ae.dataset_id
                WHERE d.method_id IS NOT NULL
                GROUP BY sg.site_id, d.method_id
            ),
            method_name_sources AS (
                SELECT site_id, method_id
                FROM site_dataset_method_links
                UNION
                SELECT sg.site_id, sg.method_id
                FROM tbl_sample_groups sg
                WHERE sg.method_id IS NOT NULL
            ),
            hits_raw AS (
                SELECT
                    mns.site_id,
                    coalesce(s.site_name, '') AS site_name,
                    coalesce(m.method_name, '') AS matched_value,
                    (
                        CASE WHEN lower(coalesce(m.method_name, '')) = lower(p.raw_query) THEN 4.5 ELSE 0 END
                        + CASE WHEN lower(coalesce(m.method_name, '')) LIKE lower(p.raw_query) || '%' THEN 2.0 ELSE 0 END
                        + CASE WHEN coalesce(m.method_name, '') ILIKE '%' || p.raw_query || '%' THEN 1.2 ELSE 0 END
                        + ts_rank_cd(setweight(to_tsvector('simple', coalesce(m.method_name, '')), 'B'), p.tsq, 32)
                    ) AS score,
                    (lower(coalesce(m.method_name, '')) = lower(p.raw_query)) AS is_exact,
                    (
                        lower(coalesce(m.method_name, '')) LIKE lower(p.raw_query) || '%'
                        AND lower(coalesce(m.method_name, '')) <> lower(p.raw_query)
                    ) AS is_prefix,
                    (
                        coalesce(m.method_name, '') ILIKE '%' || p.raw_query || '%'
                        AND NOT (lower(coalesce(m.method_name, '')) LIKE lower(p.raw_query) || '%')
                    ) AS is_contains,
                    (
                        to_tsvector('simple', coalesce(m.method_name, '')) @@ p.tsq
                        AND NOT (coalesce(m.method_name, '') ILIKE '%' || p.raw_query || '%')
                    ) AS is_fts_only,
                    NULL::integer AS sample_group_id,
                    NULL::integer AS dataset_id,
                    mns.method_id
                FROM method_name_sources mns
                JOIN tbl_methods m ON m.method_id = mns.method_id
                JOIN tbl_sites s ON s.site_id = mns.site_id
                CROSS JOIN params p
                WHERE m.method_name IS NOT NULL
                    AND (
                        to_tsvector('simple', m.method_name) @@ p.tsq
                        OR m.method_name ILIKE '%' || p.raw_query || '%'
                    )
            ),
            hits AS (
                SELECT DISTINCT ON (site_id)
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id
                FROM hits_raw
                WHERE score > 0
                ORDER BY site_id, score DESC, matched_value ASC
            ),
            total_hits AS (
                SELECT count(*)::int AS total_count
                FROM hits
            ),
            ranked AS (
                SELECT
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id,
                    row_number() OVER (
                        ORDER BY score DESC, site_name ASC
                    ) AS rn
                FROM hits
            ),
            paged_hits AS (
                SELECT
                    site_id,
                    site_name,
                    matched_value,
                    score,
                    is_exact,
                    is_prefix,
                    is_contains,
                    is_fts_only,
                    sample_group_id,
                    dataset_id,
                    method_id
                FROM ranked
                WHERE rn > $2
                    AND rn <= $3
            )
            SELECT
                t.total_count,
                ph.site_id,
                ph.site_name,
                ph.matched_value,
                ph.score,
                ph.is_exact,
                ph.is_prefix,
                ph.is_contains,
                ph.is_fts_only,
                ph.sample_group_id,
                ph.dataset_id,
                ph.method_id
            FROM total_hits t
            LEFT JOIN paged_hits ph ON true
            ORDER BY
                CASE WHEN ph.site_id IS NULL THEN 1 ELSE 0 END ASC,
                ph.score DESC,
                ph.site_name ASC
            `;
        }
        else {
            throw new Error("Unsupported category: "+categoryMeta.key);
        }

        try {
            const result = await pgClient.query(sql, [searchTerm, offset, upperBound]);
            return this.buildCategorySearchResponse(searchTerm, categoryMeta, page, perCategoryLimit, result.rows);
        }
        finally {
            this.app.releaseDbConnection(pgClient);
        }
    }
}

export default Search;
