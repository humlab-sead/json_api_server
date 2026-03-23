
import crypto from 'crypto';

export default class MCR {
    constructor(app) {
        this.app = app;

        /**
         * GET /mcr/taxon/:taxon_id
         * Returns the MCR species name, climate summary (tmax/tmin/trange ranges and centre-of-gravity
         * midpoints), and the 36×60 Birmingham Beetle Dataset climate-envelope matrix for a single
         * SEAD taxon.
         *
         * Matrix encoding (Birmingham Beetle Database):
         *   - 36 rows  → Trange axis (mean temp of warmest month − coldest month), row 1 = narrowest
         *   - 60 cols  → Tmax  axis (mean temp of warmest month), col 0 = coldest
         *   - '1' means the taxon can survive at that (Trange, Tmax) combination
         */
        this.app.expressApp.get('/mcr/taxon/:taxon_id', async (req, res) => {
            const taxonId = parseInt(req.params.taxon_id);
            if (!taxonId) {
                return res.status(400).json({ error: 'Invalid taxon_id' });
            }
            const data = await this.fetchTaxonMCR(taxonId);
            if (!data) {
                return res.status(404).json({ error: 'No MCR data found for this taxon' });
            }
            res.header('Content-Type', 'application/json');
            res.end(JSON.stringify(data, null, 2));
        });

        /**
         * GET /mcr/site/:site_id
         * Returns the Mutual Climatic Range reconstruction for a site.
         * Finds all fossil-insect (method_id=3) taxa present at the site that have MCR data,
         * then computes the element-wise intersection (logical AND) of their Birmingham Beetle
         * Dataset matrices.  The resulting matrix represents the climate space compatible with
         * every identified taxon.
         */
        this.app.expressApp.get('/mcr/site/:site_id', async (req, res) => {
            const siteId = parseInt(req.params.site_id);
            if (!siteId) {
                return res.status(400).json({ error: 'Invalid site_id' });
            }
            const data = await this.fetchSiteMCRReconstruction(siteId);
            if (!data) {
                return res.status(500).json({ error: 'Failed to fetch MCR data' });
            }
            res.header('Content-Type', 'application/json');
            res.end(JSON.stringify(data, null, 2));
        });

        /**
         * GET /mcr/dataset/:dataset_id
         * Same as the site endpoint but scoped to a single dataset.
         */
        this.app.expressApp.get('/mcr/dataset/:dataset_id', async (req, res) => {
            const datasetId = parseInt(req.params.dataset_id);
            if (!datasetId) {
                return res.status(400).json({ error: 'Invalid dataset_id' });
            }
            const data = await this.fetchDatasetMCRReconstruction(datasetId);
            if (!data) {
                return res.status(500).json({ error: 'Failed to fetch MCR data' });
            }
            res.header('Content-Type', 'application/json');
            res.end(JSON.stringify(data, null, 2));
        });

        /**
         * POST /mcr/sites
         * Body: JSON array of site_id integers, e.g. [5615, 4414, 4650]
         *
         * Returns a single aggregated MCR reconstruction across all requested sites.
         * The density_matrix counts unique taxa (deduplicated by taxon_id) that tolerate
         * each climate cell — i.e. it reflects the combined beetle fauna of all sites.
         */
        this.app.expressApp.post('/mcr/sites', async (req, res) => {
            const siteIds = req.body;
            if (!Array.isArray(siteIds) || siteIds.length === 0) {
                return res.status(400).json({ error: 'Body must be a non-empty array of site IDs' });
            }
            const parsed = siteIds.map(id => parseInt(id));
            if (parsed.some(id => !id)) {
                return res.status(400).json({ error: 'All site IDs must be integers' });
            }
            const data = await this.fetchAggregatedMCRReconstruction(parsed);
            if (!data) {
                return res.status(500).json({ error: 'Failed to fetch MCR data' });
            }
            res.header('Content-Type', 'application/json');
            res.end(JSON.stringify(data, null, 2));
        });

        /**
         * POST /mcr/sites/individual
         * Body: JSON array of site_id integers, e.g. [5615, 4414, 4650]
         *
         * Returns an array of MCR reconstructions, one entry per requested site.
         * All sites are fetched in a single database query for efficiency.
         * Sites with no MCR taxa will have reconstruction: null.
         */
        this.app.expressApp.post('/mcr/sites/individual', async (req, res) => {
            const siteIds = req.body;
            if (!Array.isArray(siteIds) || siteIds.length === 0) {
                return res.status(400).json({ error: 'Body must be a non-empty array of site IDs' });
            }
            const parsed = siteIds.map(id => parseInt(id));
            if (parsed.some(id => !id)) {
                return res.status(400).json({ error: 'All site IDs must be integers' });
            }
            const data = await this.fetchMultipleSitesMCRReconstructions(parsed);
            if (!data) {
                return res.status(500).json({ error: 'Failed to fetch MCR data' });
            }
            res.header('Content-Type', 'application/json');
            res.end(JSON.stringify(data, null, 2));
        });
    }

    mcrCacheKey(prefix, ...parts) {
        return crypto.createHash('sha256').update(prefix + JSON.stringify(parts)).digest('hex');
    }

    async fetchTaxonMCR(taxonId) {
        const cacheId = this.mcrCacheKey('mcr-taxon', taxonId);
        const identifierObject = { cache_id: cacheId };
        if (this.app.useGraphCaching) {
            const cached = await this.app.getObjectFromCache('graph_cache', identifierObject);
            if (cached !== false) return cached;
        }

        const pgClient = await this.app.getDbConnection();
        if (!pgClient) return null;

        try {
            const sql = `
                SELECT
                    mn.taxon_id,
                    mn.mcr_species_name,
                    mn.mcr_number,
                    mn.comparison_notes,
                    msd.tmax_hi, msd.tmax_lo,
                    msd.tmin_hi, msd.tmin_lo,
                    msd.trange_hi, msd.trange_lo,
                    msd.cog_mid_tmax, msd.cog_mid_trange,
                    mbd.mcr_row,
                    mbd.mcr_data
                FROM tbl_mcr_names mn
                LEFT JOIN tbl_mcr_summary_data msd ON msd.taxon_id = mn.taxon_id
                LEFT JOIN tbl_mcrdata_birmbeetledat mbd ON mbd.taxon_id = mn.taxon_id
                WHERE mn.taxon_id = $1
                ORDER BY mbd.mcr_row
            `;
            const queryResult = await pgClient.query(sql, [taxonId]);

            if (queryResult.rows.length === 0) return null;

            const first = queryResult.rows[0];
            const matrix = queryResult.rows
                .filter(r => r.mcr_row !== null)
                .map(r => r.mcr_data);

            const result = {
                cache_id: cacheId,
                taxon_id: taxonId,
                mcr_species_name: first.mcr_species_name,
                mcr_number: first.mcr_number,
                comparison_notes: first.comparison_notes,
                climate_summary: {
                    tmax_hi: first.tmax_hi,
                    tmax_lo: first.tmax_lo,
                    tmin_hi: first.tmin_hi,
                    tmin_lo: first.tmin_lo,
                    trange_hi: first.trange_hi,
                    trange_lo: first.trange_lo,
                    cog_mid_tmax: first.cog_mid_tmax,
                    cog_mid_trange: first.cog_mid_trange
                },
                matrix
            };
            if (this.app.useGraphCaching) {
                this.app.saveObjectToCache('graph_cache', identifierObject, result);
            }
            return result;
        } finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    async fetchSiteMCRReconstruction(siteId) {
        const cacheId = this.mcrCacheKey('mcr-site', siteId);
        const identifierObject = { cache_id: cacheId };
        if (this.app.useGraphCaching) {
            const cached = await this.app.getObjectFromCache('graph_cache', identifierObject);
            if (cached !== false) return cached;
        }

        const pgClient = await this.app.getDbConnection();
        if (!pgClient) return null;

        try {
            const sql = `
                SELECT DISTINCT
                    mn.taxon_id,
                    mn.mcr_species_name,
                    mn.mcr_number,
                    mbd.mcr_row,
                    mbd.mcr_data
                FROM tbl_sites s
                JOIN tbl_sample_groups sg       ON sg.site_id            = s.site_id
                JOIN tbl_physical_samples ps    ON ps.sample_group_id    = sg.sample_group_id
                JOIN tbl_analysis_entities ae   ON ae.physical_sample_id = ps.physical_sample_id
                JOIN tbl_datasets ds            ON ds.dataset_id         = ae.dataset_id
                JOIN tbl_abundances ab          ON ab.analysis_entity_id = ae.analysis_entity_id
                JOIN tbl_mcr_names mn           ON mn.taxon_id           = ab.taxon_id
                JOIN tbl_mcrdata_birmbeetledat mbd ON mbd.taxon_id       = mn.taxon_id
                WHERE ds.method_id = 3
                  AND s.site_id = $1
                ORDER BY mn.taxon_id, mbd.mcr_row
            `;
            const result = await pgClient.query(sql, [siteId]);
            const data = this.buildReconstruction(result.rows, { cache_id: cacheId, site_id: siteId });
            if (this.app.useGraphCaching) {
                this.app.saveObjectToCache('graph_cache', identifierObject, data);
            }
            return data;
        } finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    async fetchDatasetMCRReconstruction(datasetId) {
        const cacheId = this.mcrCacheKey('mcr-dataset', datasetId);
        const identifierObject = { cache_id: cacheId };
        if (this.app.useGraphCaching) {
            const cached = await this.app.getObjectFromCache('graph_cache', identifierObject);
            if (cached !== false) return cached;
        }

        const pgClient = await this.app.getDbConnection();
        if (!pgClient) return null;

        try {
            const sql = `
                SELECT DISTINCT
                    mn.taxon_id,
                    mn.mcr_species_name,
                    mn.mcr_number,
                    mbd.mcr_row,
                    mbd.mcr_data
                FROM tbl_analysis_entities ae
                JOIN tbl_abundances ab          ON ab.analysis_entity_id = ae.analysis_entity_id
                JOIN tbl_mcr_names mn           ON mn.taxon_id           = ab.taxon_id
                JOIN tbl_mcrdata_birmbeetledat mbd ON mbd.taxon_id       = mn.taxon_id
                WHERE ae.dataset_id = $1
                ORDER BY mn.taxon_id, mbd.mcr_row
            `;
            const result = await pgClient.query(sql, [datasetId]);
            const data = this.buildReconstruction(result.rows, { cache_id: cacheId, dataset_id: datasetId });
            if (this.app.useGraphCaching) {
                this.app.saveObjectToCache('graph_cache', identifierObject, data);
            }
            return data;
        } finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    async fetchAggregatedMCRReconstruction(siteIds) {
        const sortedIds = [...siteIds].sort((a, b) => a - b);
        const cacheId = this.mcrCacheKey('mcr-sites-aggregated', sortedIds);
        const identifierObject = { cache_id: cacheId };
        if (this.app.useGraphCaching) {
            const cached = await this.app.getObjectFromCache('graph_cache', identifierObject);
            if (cached !== false) return cached;
        }

        const pgClient = await this.app.getDbConnection();
        if (!pgClient) return null;

        try {
            // DISTINCT on taxon_id+mcr_row so each unique taxon is counted once
            // regardless of how many of the requested sites it appears in.
            const sql = `
                SELECT DISTINCT
                    mn.taxon_id,
                    msd.tmax_hi, msd.tmax_lo,
                    msd.tmin_hi, msd.tmin_lo,
                    msd.trange_hi, msd.trange_lo,
                    msd.cog_mid_tmax, msd.cog_mid_trange,
                    mbd.mcr_row,
                    mbd.mcr_data
                FROM tbl_sites s
                JOIN tbl_sample_groups sg          ON sg.site_id            = s.site_id
                JOIN tbl_physical_samples ps       ON ps.sample_group_id    = sg.sample_group_id
                JOIN tbl_analysis_entities ae      ON ae.physical_sample_id = ps.physical_sample_id
                JOIN tbl_datasets ds               ON ds.dataset_id         = ae.dataset_id
                JOIN tbl_abundances ab             ON ab.analysis_entity_id = ae.analysis_entity_id
                JOIN tbl_mcr_names mn              ON mn.taxon_id           = ab.taxon_id
                LEFT JOIN tbl_mcr_summary_data msd ON msd.taxon_id          = mn.taxon_id
                JOIN tbl_mcrdata_birmbeetledat mbd ON mbd.taxon_id          = mn.taxon_id
                WHERE ds.method_id = 3
                  AND s.site_id = ANY($1)
                ORDER BY mn.taxon_id, mbd.mcr_row
            `;
            const result = await pgClient.query(sql, [siteIds]);
            const data = this.buildLeanReconstruction(result.rows, { cache_id: cacheId, site_ids: siteIds });
            if (this.app.useGraphCaching) {
                this.app.saveObjectToCache('graph_cache', identifierObject, data);
            }
            return data;
        } finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    async fetchMultipleSitesMCRReconstructions(siteIds) {
        const sortedIds = [...siteIds].sort((a, b) => a - b);
        const cacheId = this.mcrCacheKey('mcr-sites-individual', sortedIds);
        const identifierObject = { cache_id: cacheId };
        if (this.app.useGraphCaching) {
            const cached = await this.app.getObjectFromCache('graph_cache', identifierObject);
            if (cached !== false) return cached.sites;
        }

        const pgClient = await this.app.getDbConnection();
        if (!pgClient) return null;

        try {
            // Use ANY($1) so all sites are fetched in one round-trip.
            // Summary data is joined to supply per-taxon climate stats for aggregation.
            const sql = `
                SELECT DISTINCT
                    s.site_id,
                    mn.taxon_id,
                    msd.tmax_hi, msd.tmax_lo,
                    msd.tmin_hi, msd.tmin_lo,
                    msd.trange_hi, msd.trange_lo,
                    msd.cog_mid_tmax, msd.cog_mid_trange,
                    mbd.mcr_row,
                    mbd.mcr_data
                FROM tbl_sites s
                JOIN tbl_sample_groups sg          ON sg.site_id            = s.site_id
                JOIN tbl_physical_samples ps       ON ps.sample_group_id    = sg.sample_group_id
                JOIN tbl_analysis_entities ae      ON ae.physical_sample_id = ps.physical_sample_id
                JOIN tbl_datasets ds               ON ds.dataset_id         = ae.dataset_id
                JOIN tbl_abundances ab             ON ab.analysis_entity_id = ae.analysis_entity_id
                JOIN tbl_mcr_names mn              ON mn.taxon_id           = ab.taxon_id
                LEFT JOIN tbl_mcr_summary_data msd ON msd.taxon_id          = mn.taxon_id
                JOIN tbl_mcrdata_birmbeetledat mbd ON mbd.taxon_id          = mn.taxon_id
                WHERE ds.method_id = 3
                  AND s.site_id = ANY($1)
                ORDER BY s.site_id, mn.taxon_id, mbd.mcr_row
            `;
            const result = await pgClient.query(sql, [siteIds]);

            // Partition rows by site_id, preserving the requested order
            const rowsBySite = new Map(siteIds.map(id => [id, []]));
            for (const row of result.rows) {
                rowsBySite.get(row.site_id).push(row);
            }

            const data = siteIds.map(siteId =>
                this.buildLeanReconstruction(rowsBySite.get(siteId), { site_id: siteId })
            );
            if (this.app.useGraphCaching) {
                this.app.saveObjectToCache('graph_cache', identifierObject, { cache_id: cacheId, sites: data });
            }
            return data;
        } finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    /**
     * Lean variant of buildReconstruction used by the multi-sites endpoint.
     * Returns only the density_matrix (taxa count per cell) — no binary matrix, no taxa list.
     * Includes aggregated climate_stats from tbl_mcr_summary_data and density_bounds
     * (bounding box + peak cell of the density matrix) for direct use by the web client.
     */
    buildLeanReconstruction(rows, meta) {
        if (rows.length === 0) {
            return { ...meta, taxa_count: 0, climate_stats: null, density_bounds: null, max_count: 0, density_matrix: null };
        }

        // Group by taxon; capture summary stats from the first row for each (they repeat per mcr_row)
        const taxaMap = new Map();
        for (const row of rows) {
            if (!taxaMap.has(row.taxon_id)) {
                taxaMap.set(row.taxon_id, {
                    tmax_hi: row.tmax_hi, tmax_lo: row.tmax_lo,
                    tmin_hi: row.tmin_hi, tmin_lo: row.tmin_lo,
                    trange_hi: row.trange_hi, trange_lo: row.trange_lo,
                    cog_mid_tmax: row.cog_mid_tmax, cog_mid_trange: row.cog_mid_trange,
                    matrixRows: []
                });
            }
            if (row.mcr_row !== null) {
                taxaMap.get(row.taxon_id).matrixRows.push({ row: row.mcr_row, data: row.mcr_data });
            }
        }

        const NUM_ROWS = 36;
        const NUM_COLS = 60;
        const counts = Array.from({ length: NUM_ROWS }, () => new Uint16Array(NUM_COLS));

        for (const taxon of taxaMap.values()) {
            for (const { row, data } of taxon.matrixRows) {
                const rowIdx = row - 1;
                for (let col = 0; col < NUM_COLS; col++) {
                    if (data[col] === '1') counts[rowIdx][col]++;
                }
            }
        }

        // Bounding box and peak cell of the density matrix
        let colMin = NUM_COLS, colMax = -1, rowMin = NUM_ROWS, rowMax = -1;
        let peakCol = 0, peakRow = 0, peakVal = 0;
        for (let r = 0; r < NUM_ROWS; r++) {
            for (let c = 0; c < NUM_COLS; c++) {
                if (counts[r][c] > 0) {
                    if (c < colMin) colMin = c;
                    if (c > colMax) colMax = c;
                    if (r < rowMin) rowMin = r;
                    if (r > rowMax) rowMax = r;
                    if (counts[r][c] > peakVal) { peakVal = counts[r][c]; peakCol = c; peakRow = r; }
                }
            }
        }
        const hasData = colMax >= 0;

        // Aggregate climate stats across all contributing taxa
        let tmaxLoMin = Infinity,  tmaxHiMax  = -Infinity;
        let tminLoMin  = Infinity,  tminHiMax  = -Infinity;
        let trangeLoMin = Infinity, trangeHiMax = -Infinity;
        let cogTmaxSum = 0, cogTrangeSum = 0, cogCount = 0;

        for (const t of taxaMap.values()) {
            if (t.tmax_lo  != null && t.tmax_lo  < tmaxLoMin)  tmaxLoMin  = t.tmax_lo;
            if (t.tmax_hi  != null && t.tmax_hi  > tmaxHiMax)  tmaxHiMax  = t.tmax_hi;
            if (t.tmin_lo  != null && t.tmin_lo  < tminLoMin)  tminLoMin  = t.tmin_lo;
            if (t.tmin_hi  != null && t.tmin_hi  > tminHiMax)  tminHiMax  = t.tmin_hi;
            if (t.trange_lo != null && t.trange_lo < trangeLoMin) trangeLoMin = t.trange_lo;
            if (t.trange_hi != null && t.trange_hi > trangeHiMax) trangeHiMax = t.trange_hi;
            if (t.cog_mid_tmax != null && t.cog_mid_trange != null) {
                cogTmaxSum   += t.cog_mid_tmax;
                cogTrangeSum += t.cog_mid_trange;
                cogCount++;
            }
        }

        return {
            ...meta,
            taxa_count: taxaMap.size,
            // Aggregated temperature ranges (°C) across all contributing taxa
            climate_stats: {
                tmax_lo:  isFinite(tmaxLoMin)   ? tmaxLoMin   : null,  // coldest lower Tmax bound across taxa
                tmax_hi:  isFinite(tmaxHiMax)   ? tmaxHiMax   : null,  // warmest upper Tmax bound across taxa
                tmin_lo:  isFinite(tminLoMin)   ? tminLoMin   : null,
                tmin_hi:  isFinite(tminHiMax)   ? tminHiMax   : null,
                trange_lo: isFinite(trangeLoMin) ? trangeLoMin : null,
                trange_hi: isFinite(trangeHiMax) ? trangeHiMax : null,
                cog_tmax_mean:   cogCount > 0 ? Math.round(cogTmaxSum   / cogCount) : null,
                cog_trange_mean: cogCount > 0 ? Math.round(cogTrangeSum / cogCount) : null
            },
            // Bounding box and hotspot of the density matrix — use for axis scaling / zoom
            density_bounds: hasData ? {
                tmax_col_min:   colMin,   // 0-based first col with any count > 0
                tmax_col_max:   colMax,   // 0-based last  col with any count > 0
                trange_row_min: rowMin,   // 0-based first row with any count > 0
                trange_row_max: rowMax,   // 0-based last  row with any count > 0
                peak_col:       peakCol,  // col of highest single-cell count
                peak_row:       peakRow,  // row of highest single-cell count
                peak_count:     peakVal   // value at peak cell
            } : null,
            max_count: taxaMap.size,      // use as denominator for colour-scale normalisation
            density_matrix: counts.map(r => Array.from(r))
        };
    }

    /**
     * Computes the element-wise intersection (logical AND) of the Birmingham Beetle Dataset
     * matrices for all taxa in `rows`, then returns a response object containing the list of
     * taxa that contributed and the resulting 36×60 matrix.
     *
     * Also reports the column (Tmax) and row (Trange) bounds of the non-zero region so the
     * client can quickly derive the estimated climate range without scanning the full matrix.
     */
    buildReconstruction(rows, meta) {
        if (rows.length === 0) {
            return { ...meta, taxa_count: 0, taxa: [], reconstruction: null };
        }

        // Group matrix rows by taxon
        const taxaMap = new Map();
        for (const row of rows) {
            if (!taxaMap.has(row.taxon_id)) {
                taxaMap.set(row.taxon_id, {
                    taxon_id: row.taxon_id,
                    mcr_species_name: row.mcr_species_name,
                    mcr_number: row.mcr_number,
                    matrixRows: []
                });
            }
            if (row.mcr_row !== null) {
                taxaMap.get(row.taxon_id).matrixRows.push({ row: row.mcr_row, data: row.mcr_data });
            }
        }

        const NUM_ROWS = 36;
        const NUM_COLS = 60;

        // Start with all 1s for intersection; 0s for counts.
        // Both are built in a single pass over the taxa.
        const intersection = Array.from({ length: NUM_ROWS }, () => new Uint8Array(NUM_COLS).fill(1));
        const counts       = Array.from({ length: NUM_ROWS }, () => new Uint16Array(NUM_COLS));

        for (const taxon of taxaMap.values()) {
            for (const { row, data } of taxon.matrixRows) {
                const rowIdx = row - 1; // mcr_row is 1-indexed
                for (let col = 0; col < NUM_COLS; col++) {
                    if (data[col] === '1') {
                        counts[rowIdx][col]++;
                    } else {
                        intersection[rowIdx][col] = 0;
                    }
                }
            }
        }

        const matrixStrings = intersection.map(r => Array.from(r).join(''));
        // density_matrix: 36 arrays of 60 integers — how many taxa tolerate each (Trange, Tmax) cell
        const densityMatrix = counts.map(r => Array.from(r));

        // Find the bounding box of the viable (value=1) region in the intersection matrix
        let tmaxColMin = NUM_COLS, tmaxColMax = -1;
        let trangeRowMin = NUM_ROWS, trangeRowMax = -1;
        for (let r = 0; r < NUM_ROWS; r++) {
            for (let c = 0; c < NUM_COLS; c++) {
                if (intersection[r][c] === 1) {
                    if (c < tmaxColMin) tmaxColMin = c;
                    if (c > tmaxColMax) tmaxColMax = c;
                    if (r < trangeRowMin) trangeRowMin = r;
                    if (r > trangeRowMax) trangeRowMax = r;
                }
            }
        }

        const hasResult = tmaxColMax >= 0;
        const taxa = Array.from(taxaMap.values()).map(t => ({
            taxon_id: t.taxon_id,
            mcr_species_name: t.mcr_species_name,
            mcr_number: t.mcr_number
        }));

        return {
            ...meta,
            taxa_count: taxaMap.size,
            taxa,
            reconstruction: {
                // 36 strings of 60 '0'/'1' chars: cell is '1' only where ALL taxa tolerate that climate point
                matrix: matrixStrings,
                // 0-based column indices bounding the viable Tmax range in the intersection matrix
                viable_tmax_cols: hasResult ? [tmaxColMin, tmaxColMax] : null,
                // 0-based row indices bounding the viable Trange range in the intersection matrix
                viable_trange_rows: hasResult ? [trangeRowMin, trangeRowMax] : null,
                // 36 arrays of 60 integers: how many taxa tolerate each (Trange, Tmax) cell
                density_matrix: densityMatrix,
                // maximum value in density_matrix — use for normalising colour scale
                max_count: taxaMap.size
            }
        };
    }
}

