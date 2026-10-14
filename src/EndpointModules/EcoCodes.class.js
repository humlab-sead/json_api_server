import Taxa from './Taxa.class.js';

class EcoCodes {
    constructor(app) {
        this.app = app;
        this.taxaModule = new Taxa(app);

        //The sample_ecocode_vectors collection, held in memory and grouped by site_id since every map request reads a large part of it
        this.sampleVectorsBySite = null;
        this.sampleVectorsLoading = null;
        this.mapReference = null;
        //Sites with fewer individuals than this are too small to say what is typical for an ecocode
        this.mapReferenceMinIndividuals = 30;

        this.app.expressApp.get('/ecocodes/taxon/:taxonid', async (req, res) => {
            let taxonId = parseInt(req.params.taxonid);
            if(taxonId) {
                let ecoCodes = await this.fetchEcoCodesForTaxon(taxonId);
                res.header("Content-type", "application/json");
                res.end(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/ecocodes/site/:siteid', async (req, res) => {
            let siteId = parseInt(req.params.siteid);
            if(siteId) {
                let ecoCodes = await this.getEcoCodesForSite(siteId);
                res.header("Content-type", "application/json");
                res.end(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/ecocodes/summary/site/:siteid', async (req, res) => {
            let siteId = parseInt(req.params.siteid);
            if(siteId) {
                let ecoCodes = await this.getEcoCodesSummaryForSite(siteId);
                res.header("Content-type", "application/json");
                res.end(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/ecocodes/dataset/:datasetid', async (req, res) => {
            let datasetId = parseInt(req.params.datasetid);
            if(datasetId) {
                let ecoCodes = await this.getEcoCodesForDataset(datasetId);
                res.header("Content-type", "application/json");
                res.end(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/ecocodes/dataset/:datasetid/samples', async (req, res) => {
            let datasetId = parseInt(req.params.datasetid);
            if(datasetId) {
                let ecoCodes = await this.getEcoCodesForDatasetBySample(datasetId);
                res.header("Content-type", "application/json");
                res.end(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/ecocodes/site/:siteid/samples', async (req, res) => {
            let siteId = parseInt(req.params.siteid);
            if(siteId) {
                let ecoCodes = await this.getEcoCodesForSiteGroupedBySample(siteId);
                res.header("Content-type", "application/json");
                res.end(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/preload/ecocodes/:flushCache?', async (req, res) => {
            console.log(req.path);
            if(req.params.flushCache) {
                //await this.flushEcocodeCache(); //not implemented
            }
            await this.preloadAllSiteEcocodes();
            res.end("Preload of ecocodes complete");
        });

        this.app.expressApp.post('/ecocodes/sites/individual/:ecocodeId', async (req, res) => { //the endpoint url for this seems kinda dumb, just saying
            let siteIds = req.body;
            if(!this.isValidSiteIdsInput(siteIds)) {
                res.status(400);
                res.send("Bad input - should be an array of site IDs");
                return;
            }

            let summaries = await this.getSingleEcoCodeForSites(siteIds, req.params.ecocodeId);

            res.header("Content-type", "application/json");
            res.end(JSON.stringify(summaries, null, 2));
        });

        /*
        * The ecocode values of each site for the result map's ecocode layers. Body: { site_ids: [...], time_window: [younger, older] }
        * where the time window (years BP) is optional - with one, only the samples dated to within it are counted, and undated samples are left out.
        */
        this.app.expressApp.post('/ecocodes/map/sites', async (req, res) => {
            let siteIds = req.body ? req.body.site_ids : null;
            if(!this.isValidSiteIdsInput(siteIds)) {
                res.status(400);
                res.send("Bad input - site_ids should be an array of site IDs");
                return;
            }

            let timeWindow = this.parseTimeWindow(req.body.time_window);
            if(timeWindow === false) {
                res.status(400);
                res.send("Bad input - time_window should be [younger, older] in years BP");
                return;
            }

            try {
                let data = await this.getEcoCodeMapData(siteIds.map(siteId => Number(siteId)), timeWindow);
                res.header("Content-type", "application/json");
                res.end(JSON.stringify(data));
            }
            catch(error) {
                console.error("Failed to compile ecocode map data:", error);
                res.status(500);
                res.send("Failed to compile ecocode map data");
            }
        });

        this.app.expressApp.post('/ecocodes/sites/:aggregationType', async (req, res) => {
            let siteIds = req.body;
            if(!this.isValidSiteIdsInput(siteIds)) {
                res.status(400);
                res.send("Bad input - should be an array of site IDs");
                return;
            }

            let data = await this.fetchEcoCodesForSites(siteIds, req.params.aggregationType);
            res.header("Content-type", "application/json");
            res.end(JSON.stringify(data, null, 2));
        });
    }

    isValidSiteIdsInput(siteIds) {
        if(!Array.isArray(siteIds)) {
            return false;
        }

        return siteIds.every(siteId => {
            const numericSiteId = Number(siteId);
            return Number.isInteger(numericSiteId) && numericSiteId > 0;
        });
    }

    async flushEcocodeCache() {
        console.log("Flushing ecocode cache");
        if(this.cacheStorageMethod == "mongo") {
            await this.mongo.collection('site_ecocode_bundles').deleteMany({});
        }
    }

    async preloadAllSiteEcocodes(flushCache = false) {

        if(flushCache) {
            await this.flushEcocodeCache();
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        console.time("Preload of ecocodes complete");
        console.log("Preloading ecocodes");

        let siteData = await pgClient.query('SELECT * FROM tbl_sites ORDER BY site_id');
        this.app.releaseDbConnection(pgClient);

        console.log("Will fetch ecocodes data for "+siteData.rows.length+" sites");

        let siteIds = [];
        for(let key in siteData.rows) {
            siteIds.push(siteData.rows[key].site_id);
        }

        let pendingFetches = 0;

        await new Promise((resolve, reject) => {
            const fetchCheckInterval = setInterval(() => {
                if(siteIds.length > 0 && pendingFetches < this.app.maxConcurrentFetches) {
                    pendingFetches++;
                    let siteId = siteIds.shift();
                    console.time("Fetched ecocodes for site "+siteId);
                    this.getEcoCodesForSite(siteId).then(() => {
                        console.timeEnd("Fetched ecocodes for site "+siteId);
                        pendingFetches--;
                    });
                }
                if(siteIds.length == 0) {
                    clearInterval(fetchCheckInterval);
                    resolve();
                }
            }, 100);
        });

        console.timeEnd("Preload of ecocodes complete");

        await this.preloadSampleEcocodeVectors();
    }

    /*
    * Function: preloadSampleEcocodeVectors
    *
    * Builds the sample_ecocode_vectors collection - one document per palaeoentomology sample with its abundance weighted Bugs ecocode
    * values (the same values as the ecocode charts aggregate), its number of individuals and its age in years BP. The ages are taken
    * from the cached sites' age_summary, so the sites must have been preloaded first.
    */
    async preloadSampleEcocodeVectors() {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        console.time("Preload of sample ecocode vectors complete");

        let sampleSql = `SELECT
        tbl_sample_groups.site_id,
        tbl_analysis_entities.physical_sample_id,
        SUM(tbl_abundances.abundance) AS individuals
        FROM tbl_datasets
        JOIN tbl_analysis_entities ON tbl_analysis_entities.dataset_id=tbl_datasets.dataset_id
        JOIN tbl_physical_samples ON tbl_physical_samples.physical_sample_id=tbl_analysis_entities.physical_sample_id
        JOIN tbl_sample_groups ON tbl_sample_groups.sample_group_id=tbl_physical_samples.sample_group_id
        JOIN tbl_abundances ON tbl_abundances.analysis_entity_id=tbl_analysis_entities.analysis_entity_id
        WHERE tbl_datasets.method_id=3
        GROUP BY tbl_sample_groups.site_id, tbl_analysis_entities.physical_sample_id`;

        let ecoCodeSql = `SELECT
        tbl_analysis_entities.physical_sample_id,
        tbl_ecocodes.ecocode_definition_id,
        SUM(tbl_abundances.abundance) AS abundance
        FROM tbl_datasets
        JOIN tbl_analysis_entities ON tbl_analysis_entities.dataset_id=tbl_datasets.dataset_id
        JOIN tbl_abundances ON tbl_abundances.analysis_entity_id=tbl_analysis_entities.analysis_entity_id
        JOIN tbl_ecocodes ON tbl_ecocodes.taxon_id=tbl_abundances.taxon_id
        JOIN tbl_ecocode_definitions ON tbl_ecocode_definitions.ecocode_definition_id=tbl_ecocodes.ecocode_definition_id
        JOIN tbl_ecocode_groups ON tbl_ecocode_groups.ecocode_group_id=tbl_ecocode_definitions.ecocode_group_id
        WHERE tbl_datasets.method_id=3 AND tbl_ecocode_groups.ecocode_system_id=2
        GROUP BY tbl_analysis_entities.physical_sample_id, tbl_ecocodes.ecocode_definition_id`;

        let sampleResult = null;
        let ecoCodeResult = null;
        try {
            sampleResult = await pgClient.query(sampleSql);
            ecoCodeResult = await pgClient.query(ecoCodeSql);
        }
        finally {
            this.app.releaseDbConnection(pgClient);
        }

        let samples = new Map();
        sampleResult.rows.forEach(row => {
            samples.set(Number(row.physical_sample_id), {
                site_id: Number(row.site_id),
                physical_sample_id: Number(row.physical_sample_id),
                individuals: Number(row.individuals) || 0,
                older: null,
                younger: null,
                ecocodes: {}
            });
        });

        ecoCodeResult.rows.forEach(row => {
            let sample = samples.get(Number(row.physical_sample_id));
            if(sample) {
                sample.ecocodes[row.ecocode_definition_id] = Number(row.abundance) || 0;
            }
        });

        let siteIds = [...new Set([...samples.values()].map(sample => sample.site_id))];
        let sites = await this.app.mongo.collection('sites').find({ site_id: { $in: siteIds } }, { projection: { site_id: 1, age_summary: 1 } }).toArray();
        sites.forEach(site => {
            this.applySampleAges(samples, site.age_summary);
        });

        let apiSource = this.app.appName+"-"+this.app.appVersion;
        let documents = [...samples.values()].map(sample => {
            sample.api_source = apiSource;
            return sample;
        });

        let collection = this.app.mongo.collection('sample_ecocode_vectors');
        await collection.deleteMany({});
        if(documents.length > 0) {
            await collection.insertMany(documents);
        }
        await collection.createIndex({ site_id: 1 });

        this.sampleVectorsBySite = null;
        this.mapReference = null;

        let datedCount = documents.filter(sample => sample.older !== null).length;
        console.log("Stored ecocode vectors for "+documents.length+" samples ("+datedCount+" dated) from "+siteIds.length+" sites");
        console.timeEnd("Preload of sample ecocode vectors complete");
        return documents.length;
    }

    /*
    * Function: applySampleAges
    *
    * Gives each sample the span of the ages in the site's age_summary that belong to it. An age with an open bound counts as the bound it has.
    */
    applySampleAges(samples, ageSummary) {
        if(!ageSummary || !Array.isArray(ageSummary.datings)) {
            return;
        }

        ageSummary.datings.forEach(dating => {
            (dating.ages || []).forEach(age => {
                let sample = samples.get(Number(age.physical_sample_id));
                if(!sample) {
                    return;
                }

                let older = age.older !== null && typeof age.older != "undefined" ? Number(age.older) : null;
                let younger = age.younger !== null && typeof age.younger != "undefined" ? Number(age.younger) : null;
                if(older === null) {
                    older = younger;
                }
                if(younger === null) {
                    younger = older;
                }
                if(older === null || Number.isNaN(older) || Number.isNaN(younger)) {
                    return;
                }

                sample.older = sample.older === null ? older : Math.max(sample.older, older);
                sample.younger = sample.younger === null ? younger : Math.min(sample.younger, younger);
            });
        });
    }

    /*
    * Function: parseTimeWindow
    *
    * Returns the time window as { younger, older } in years BP, null if there is none, or false if it is malformed.
    */
    parseTimeWindow(timeWindow) {
        if(timeWindow === null || typeof timeWindow == "undefined") {
            return null;
        }
        if(!Array.isArray(timeWindow) || timeWindow.length != 2) {
            return false;
        }
        let values = timeWindow.map(value => Number(value));
        if(values.some(value => !Number.isFinite(value))) {
            return false;
        }
        return {
            younger: Math.min(values[0], values[1]),
            older: Math.max(values[0], values[1])
        };
    }

    async getSampleVectorsBySite() {
        if(this.sampleVectorsBySite) {
            return this.sampleVectorsBySite;
        }
        if(this.sampleVectorsLoading) {
            return this.sampleVectorsLoading;
        }

        this.sampleVectorsLoading = (async () => {
            let collection = this.app.mongo.collection('sample_ecocode_vectors');
            let first = await collection.findOne({});
            if(!first || !this.app.checkVersionMatch(first.api_source, this.app.appName+"-"+this.app.appVersion)) {
                await this.preloadSampleEcocodeVectors();
            }

            let documents = await collection.find({}, { projection: { _id: 0, api_source: 0 } }).toArray();
            this.sampleVectorsBySite = this.groupSampleVectorsBySite(documents);
            return this.sampleVectorsBySite;
        })();

        try {
            return await this.sampleVectorsLoading;
        }
        finally {
            this.sampleVectorsLoading = null;
        }
    }

    groupSampleVectorsBySite(samples) {
        let samplesBySite = new Map();
        samples.forEach(sample => {
            if(!samplesBySite.has(sample.site_id)) {
                samplesBySite.set(sample.site_id, []);
            }
            samplesBySite.get(sample.site_id).push(sample);
        });
        return samplesBySite;
    }

    async getEcoCodeMapData(siteIds, timeWindow = null) {
        let samplesBySite = await this.getSampleVectorsBySite();
        if(!this.mapReference) {
            this.mapReference = this.compileMapReference(samplesBySite);
        }

        let mapData = this.aggregateSiteEcoCodes(samplesBySite, siteIds, timeWindow);
        mapData.api_source = this.app.appName+"-"+this.app.appVersion;
        mapData.reference = this.mapReference;
        return mapData;
    }

    /*
    * Function: aggregateSiteEcoCodes
    *
    * Sums the samples of each site into its ecocode values. With a time window, only the samples whose age overlaps it are counted.
    */
    aggregateSiteEcoCodes(samplesBySite, siteIds, timeWindow = null) {
        let sampleCounts = {
            included: 0,
            undated: 0,
            outside_time_window: 0
        };
        let sites = [];

        siteIds.forEach(siteId => {
            let samples = samplesBySite.get(siteId);
            if(!samples) {
                return;
            }

            let site = {
                site_id: siteId,
                samples: 0,
                individuals: 0,
                ecocodes: {}
            };

            samples.forEach(sample => {
                if(timeWindow) {
                    if(sample.older === null) {
                        sampleCounts.undated++;
                        return;
                    }
                    if(sample.older < timeWindow.younger || sample.younger > timeWindow.older) {
                        sampleCounts.outside_time_window++;
                        return;
                    }
                }

                site.samples++;
                site.individuals += sample.individuals;
                for(let ecocodeId in sample.ecocodes) {
                    site.ecocodes[ecocodeId] = (site.ecocodes[ecocodeId] || 0) + sample.ecocodes[ecocodeId];
                }
            });

            if(site.samples > 0) {
                sampleCounts.included += site.samples;
                sites.push(site);
            }
        });

        return {
            time_window: timeWindow ? [timeWindow.younger, timeWindow.older] : null,
            sample_counts: sampleCounts,
            sites: sites
        };
    }

    /*
    * Function: compileMapReference
    *
    * What is typical for each ecocode across all sites, whatever their age: the mean and the 95th percentile of its share of the site's
    * sumrep. The map uses these as fixed colour scales, so that a colour means the same whatever is filtered.
    */
    compileMapReference(samplesBySite) {
        let siteIds = [...samplesBySite.keys()];
        let siteData = this.aggregateSiteEcoCodes(samplesBySite, siteIds).sites;

        let sharesByEcocode = {};
        let siteCount = 0;
        siteData.forEach(site => {
            if(site.individuals < this.mapReferenceMinIndividuals) {
                return;
            }
            let sumrep = Object.values(site.ecocodes).reduce((sum, value) => sum + value, 0);
            if(sumrep <= 0) {
                return;
            }
            siteCount++;
            for(let ecocodeId in site.ecocodes) {
                if(!sharesByEcocode[ecocodeId]) {
                    sharesByEcocode[ecocodeId] = [];
                }
                sharesByEcocode[ecocodeId].push(site.ecocodes[ecocodeId] / sumrep);
            }
        });

        let ecocodes = {};
        for(let ecocodeId in sharesByEcocode) {
            //Sites lacking the ecocode have a share of 0
            let shares = sharesByEcocode[ecocodeId].concat(new Array(siteCount - sharesByEcocode[ecocodeId].length).fill(0));
            shares.sort((a, b) => a - b);
            ecocodes[ecocodeId] = {
                mean: shares.reduce((sum, share) => sum + share, 0) / shares.length,
                p95: shares[Math.min(shares.length - 1, Math.floor(shares.length * 0.95))]
            };
        }

        return {
            site_count: siteCount,
            min_individuals: this.mapReferenceMinIndividuals,
            ecocodes: ecocodes
        };
    }

    async fetchEcoCodesForTaxon(taxonId) {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = `SELECT 
        tbl_ecocodes.taxon_id,
        tbl_ecocode_definitions.ecocode_definition_id,
        tbl_ecocode_definitions.abbreviation,
        tbl_ecocode_definitions.definition,
        tbl_ecocode_definitions.name,
        tbl_ecocode_definitions.notes,
        tbl_ecocode_definitions.sort_order,
        tbl_ecocode_groups.ecocode_group_id,
        tbl_ecocode_groups.ecocode_system_id,
        tbl_ecocode_groups.name as group_name,
        tbl_ecocode_groups.definition as group_definition
        FROM tbl_ecocodes 
        LEFT JOIN tbl_ecocode_definitions ON tbl_ecocode_definitions.ecocode_definition_id=tbl_ecocodes.ecocode_definition_id
        LEFT JOIN tbl_ecocode_groups ON tbl_ecocode_groups.ecocode_group_id=tbl_ecocode_definitions.ecocode_group_id
        WHERE tbl_ecocode_groups.ecocode_system_id=2 AND tbl_ecocodes.taxon_id=$1`;
        let result = await pgClient.query(sql, [taxonId]);
        this.app.releaseDbConnection(pgClient);

        let taxon = {
            taxon_id: taxonId,
            ecocodes: result.rows
        };

        return taxon;
    }

    async fetchEcoCodesForSites(siteIds, aggregationType = "abundance") {
        //aggregationType can be "abundance" or "species"
        //an abundance aggregation will give us the ecocodes based on the abundance weight of each species
        //a species aggregation ignores abundance and just gives us the aggregated ecocodes considering only the species included, not their abundance


        //fetch the list of aggregated species for all our sites
        //this will only give us the most abundant species, which in turn will give us a less accurate ecocode chart,
        //but it will hopefully be good enough and we will need to do it this way for performance reasons
        
        let topTaxa = await this.taxaModule.fetchTaxaForSites(siteIds, 20);

        let promises = [];
        for(let key in topTaxa) {
            let taxon = topTaxa[key];
            let p = this.fetchEcoCodesForTaxon(taxon.taxon_id).then(taxonEcoCodes => {
                taxon.ecocodes = taxonEcoCodes.ecocodes;
            });
            promises.push(p);
        }

        await Promise.all(promises);

        //now we have the top 20 taxa for each site, and the ecocodes for each of those taxa
        //we need to aggregate the ecocodes for each site

        let aggregatedEcocodes = [];

        for (let taxon of topTaxa) {
            let ecocodes = taxon.ecocodes;
    
            if (aggregationType === "abundance") {
                // Aggregating based on abundance
                for (let ecoCode of ecocodes) {
                    let existingEcocode = aggregatedEcocodes.find(e => e.ecocode_definition_id === ecoCode.ecocode_definition_id);
                    if (existingEcocode) {
                        existingEcocode.species += 1;
                        existingEcocode.abundance += taxon.abundance;
                    } else {
                        aggregatedEcocodes.push({
                            ecocode_definition_id: ecoCode.ecocode_definition_id,
                            species: 1,
                            abundance: taxon.abundance
                        });
                    }
                }
            } else if (aggregationType === "species") {
                // Aggregating based on species
                for (let ecoCode of ecocodes) {
                    let existingEcocode = aggregatedEcocodes.find(e => e.ecocode_definition_id === ecoCode.ecocode_definition_id);
                    if (existingEcocode) {
                        existingEcocode.species += 1;
                    } else {
                        aggregatedEcocodes.push({
                            ecocode_definition_id: ecoCode.ecocode_definition_id,
                            species: 1,
                            abundance: 0
                        });
                    }
                }
            }
        }
    
        return aggregatedEcocodes;
    }

    async getEcoCodesForDataset(datasetId) {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let aeResult = await pgClient.query(
            'SELECT * FROM tbl_analysis_entities WHERE dataset_id=$1',
            [datasetId]
        );

        let abundancePromises = aeResult.rows.map(ae =>
            pgClient.query('SELECT * FROM tbl_abundances WHERE analysis_entity_id=$1', [ae.analysis_entity_id])
        );
        let abundanceResults = await Promise.all(abundancePromises);
        this.app.releaseDbConnection(pgClient);

        let allAbundances = abundanceResults.flatMap(r => r.rows);

        let fetchPromises = [];
        let ecocodeBundles = [];

        allAbundances.forEach(abundance => {
            let promise = this.fetchEcoCodesForTaxon(abundance.taxon_id);
            fetchPromises.push(promise);
            promise.then(ecoCodes => {
                if(ecoCodes === false) {
                    return;
                }
                ecoCodes.ecocodes.forEach(ecoCode => {
                    let bundleFound = false;
                    for(let bundleKey in ecocodeBundles) {
                        if(ecocodeBundles[bundleKey].ecocode.ecocode_definition_id == ecoCode.ecocode_definition_id) {
                            bundleFound = true;
                            if(!ecocodeBundles[bundleKey].taxa.includes(abundance.taxon_id)) {
                                ecocodeBundles[bundleKey].taxa.push(abundance.taxon_id);
                            }
                            ecocodeBundles[bundleKey].abundance += abundance.abundance;
                        }
                    }
                    if(!bundleFound) {
                        ecocodeBundles.push({
                            ecocode: ecoCode,
                            taxa: [abundance.taxon_id],
                            abundance: abundance.abundance
                        });
                    }
                });
            });
        });

        await Promise.all(fetchPromises);

        return {
            api_source: this.app.appName+"-"+this.app.appVersion,
            dataset_id: datasetId,
            ecocode_bundles: ecocodeBundles
        };
    }

    async getEcoCodesForDatasetBySample(datasetId) {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let aeResult = await pgClient.query(
            'SELECT * FROM tbl_analysis_entities WHERE dataset_id=$1',
            [datasetId]
        );

        let abundancePromises = aeResult.rows.map(ae =>
            pgClient.query('SELECT * FROM tbl_abundances WHERE analysis_entity_id=$1', [ae.analysis_entity_id])
                .then(r => ({ physical_sample_id: ae.physical_sample_id, rows: r.rows }))
        );
        let abundanceResults = await Promise.all(abundancePromises);
        this.app.releaseDbConnection(pgClient);

        // Build a map of physical_sample_id -> abundances
        let sampleMap = {};
        for(let { physical_sample_id, rows } of abundanceResults) {
            if(!sampleMap[physical_sample_id]) {
                sampleMap[physical_sample_id] = [];
            }
            sampleMap[physical_sample_id].push(...rows);
        }

        let sampleEcoCodes = Object.keys(sampleMap).map(id => ({
            physical_sample_id: parseInt(id),
            ecocodes: []
        }));

        let fetchPromises = [];

        sampleEcoCodes.forEach(sampleEcoCode => {
            sampleMap[sampleEcoCode.physical_sample_id].forEach(abundance => {
                let promise = this.fetchEcoCodesForTaxon(abundance.taxon_id);
                fetchPromises.push(promise);
                promise.then(ecoCodes => {
                    if(ecoCodes === false) {
                        return;
                    }
                    ecoCodes.ecocodes.forEach(ecoCode => {
                        let bundleFound = false;
                        for(let bundleKey in sampleEcoCode.ecocodes) {
                            if(sampleEcoCode.ecocodes[bundleKey].ecocode.ecocode_definition_id == ecoCode.ecocode_definition_id) {
                                bundleFound = true;
                                if(!sampleEcoCode.ecocodes[bundleKey].taxa.includes(abundance.taxon_id)) {
                                    sampleEcoCode.ecocodes[bundleKey].taxa.push(abundance.taxon_id);
                                }
                                sampleEcoCode.ecocodes[bundleKey].abundance += abundance.abundance;
                            }
                        }
                        if(!bundleFound) {
                            sampleEcoCode.ecocodes.push({
                                ecocode: ecoCode,
                                taxa: [abundance.taxon_id],
                                abundance: abundance.abundance
                            });
                        }
                    });
                });
            });
        });

        await Promise.all(fetchPromises);

        return {
            api_source: this.app.appName+"-"+this.app.appVersion,
            dataset_id: datasetId,
            ecocode_bundles: sampleEcoCodes
        };
    }

    async getEcoCodesForSample(physicalSampleId) {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        //1. select * from tbl_analysis_entities where physical_sample_id=39065 and dataset_id=34569
        let sql = `
        SELECT * from tbl_analysis_entities 
        LEFT JOIN tbl_datasets ON tbl_datasets.dataset_id=tbl_analysis_entities.dataset_id
        WHERE physical_sample_id=$1 AND tbl_datasets.method_id=3`;
        let result = await pgClient.query(sql, [physicalSampleId]);
        
        let promises = [];
        result.rows.forEach(row => {
            //2. select * from tbl_abundances where analysis_entity_id=147992 - this will get the abundances as well as the taxon_id's
            promises.push(pgClient.query("SELECT * FROM tbl_abundances WHERE analysis_entity_id=$1", [row.analysis_entity_id]));
        });
        let taxaAbundancesResult = await Promise.all(promises);
        this.app.releaseDbConnection(pgClient);

        taxaAbundancesResult.rows.forEach(taxonAbundance => {
            taxonAbundance.taxon_id;
            taxonAbundance.abundance;
            taxonAbundance.abundance_element_id;
            this.fetchEcoCodesForTaxon(taxonAbundance.taxon_id).then(taxonEcoCodes => {
                
            });
        });
    }

    async getEcoCodesForSiteGroupedBySample(siteId) {
        if(this.app.useEcoCodeCaching) {
            let ecoCodeSiteBundle = await this.app.getObjectFromCache("site_ecocode_sample_bundles", { site_id: parseInt(siteId) });
            if(ecoCodeSiteBundle) {
                return ecoCodeSiteBundle;
            }
        }

        let site = await this.app.getObjectFromCache("sites", { site_id: siteId });
        if(!site) {
            //If we don't have this site cached we give up on this request (at the moment!), but should perhaps be handled better in the future
            console.warn("Site "+siteId+" is not cached");
            return [];
        }
        
        //Filter out only abundance counting datasets
        let datasets = site.datasets.filter(dataset => {
            return dataset.method_id == 3; //method_id 3 is paleoentomology
        });

        //aggregation modes: "numberOfSpecies" (per eco code) or "countOfAbundace" (per eco code)

        /*
        {
            api_source: <version>,
            samples: [
                {
                    physical_sample_id: ,
                    ecocodes: [
                        {
                            ecocode_id,
                            species: 3,
                            abundance: 32 //This is, obviously, the aggregated abundance of all the specices that can be linked to this ecocode, obviously. Again, obviously (obviously).
                        },
                    ],
                }
            ]
        }
        */

        let fetchPromises = [];

        let sampleIds = [];
        let sampleEcoCodes = [];

        datasets.forEach(dataset => {
            dataset.analysis_entities.forEach(ae => {
                if(!sampleIds.includes(ae.physical_sample_id)) {
                    sampleIds.push(ae.physical_sample_id);
                }
            })
        });

        sampleIds.forEach(sampleId => {
            sampleEcoCodes.push({
                physical_sample_id: sampleId,
                ecocodes: []
            });
        });
        
        sampleEcoCodes.forEach(sampleEcoCode => {
            datasets.forEach(dataset => {
                dataset.analysis_entities.forEach(ae => {
                    if(ae.physical_sample_id == sampleEcoCode.physical_sample_id) {
                        ae.abundances.forEach(abundance => {
                            
                            abundance.taxon_id;
                            abundance.abundance;

                            let promise = this.fetchEcoCodesForTaxon(abundance.taxon_id);
                            fetchPromises.push(promise);
                            promise.then(ecoCodes => {
                                if(ecoCodes === false) {
                                    return {
                                        status: "error",
                                        msg: "Could not get eco codes for taxon "+abundance.taxon_id
                                    };
                                }
                                ecoCodes.ecocodes.forEach(ecoCode => {
                                    let bundleFound = false;
                                    for(let bundleKey in sampleEcoCode.ecocodes) {
                                        if(sampleEcoCode.ecocodes[bundleKey].ecocode.ecocode_definition_id == ecoCode.ecocode_definition_id) {
                                            bundleFound = true;
                                            if(!sampleEcoCode.ecocodes[bundleKey].taxa.includes(abundance.taxon_id)) {
                                                sampleEcoCode.ecocodes[bundleKey].taxa.push(abundance.taxon_id);
                                            }
                                            sampleEcoCode.ecocodes[bundleKey].abundance += abundance.abundance;
                                        }
                                    }
                                    if(!bundleFound) {
                                        sampleEcoCode.ecocodes.push({
                                            ecocode: ecoCode,
                                            taxa: [abundance.taxon_id],
                                            abundance: abundance.abundance
                                        });
                                    }
                                });
                            });
                        });
                    }
                })
            });
        });


        await Promise.all(fetchPromises);

        let bundleObject = {
            api_source: this.app.appName+"-"+this.app.appVersion,
            site_id: siteId,
            ecocode_bundles: sampleEcoCodes
        };

        if(this.app.useEcoCodeCaching) {
            this.app.saveObjectToCache("site_ecocode_sample_bundles", { site_id: parseInt(siteId) }, bundleObject);
        }
        
        return bundleObject;
    }

    async getSingleEcoCodeForSites(siteIds, ecocodeId = null) {
        const pipeline = [
            // Match the document corresponding to the specific siteId
            { $match: { site_id: { $in: siteIds } } },
    
            // Unwind the ecocode_bundles array to process each ecocode bundle individually
            { $unwind: '$ecocode_bundles' },
    
            // Optional: If you want to gather more details or manipulate data, add further stages here
            { $match: { 'ecocode_bundles.ecocode.ecocode_definition_id': parseInt(ecocodeId) } },
    
            // Group the data by ecocode_definition_id and gather relevant information
            { 
                $group: {
                    _id: '$site_id', // Group by site ID
                    site_id: { $first: '$site_id' }, // Get the site ID
                    abbreviation: { $first: '$ecocode_bundles.ecocode.abbreviation' }, // Get the abbreviation of the ecocode
                    definition: { $first: '$ecocode_bundles.ecocode.definition' }, // Get the definition of the ecocode
                    name: { $first: '$ecocode_bundles.ecocode.name' }, // Get the name of the ecocode
                    ecocode_definition_id: { $first: '$ecocode_bundles.ecocode.ecocode_definition_id' }, // Get the ecocode definition ID
                    totalAbundance: { $sum: '$ecocode_bundles.abundance' }, // Sum the abundance values
                    taxaCount: { $sum: { $size: '$ecocode_bundles.taxa' } }, // Count the total number of taxa
                }
            },
    
            // Sort the results by totalAbundance in descending order
            { $sort: { totalAbundance: -1 } }
        ];

        try {
            const result = await this.app.mongo.collection('site_ecocode_bundles').aggregate(pipeline).toArray();
            return result;
        } catch (error) {
            console.error("Failed to retrieve EcoCodes summary:", error);
            throw error;
        }

    }

    async getEcoCodesSummaryForSite(siteId) {
        const pipeline = [
            // Match the document corresponding to the specific siteId
            { $match: { site_id: siteId } },
    
            // Unwind the ecocode_bundles array to process each ecocode bundle individually
            { $unwind: '$ecocode_bundles' },
    
            // Optional: If you want to gather more details or manipulate data, add further stages here
    
            // Group the data by ecocode_definition_id and gather relevant information
            { 
                $group: {
                    _id: '$ecocode_bundles.ecocode.ecocode_definition_id', // Group by ecocode definition ID
                    abbreviation: { $first: '$ecocode_bundles.ecocode.abbreviation' }, // Get the abbreviation of the ecocode
                    definition: { $first: '$ecocode_bundles.ecocode.definition' }, // Get the definition of the ecocode
                    name: { $first: '$ecocode_bundles.ecocode.name' }, // Get the name of the ecocode
                    totalAbundance: { $sum: '$ecocode_bundles.abundance' }, // Sum the abundance values
                    taxaCount: { $sum: { $size: '$ecocode_bundles.taxa' } }, // Count the total number of taxa
                }
            },
    
            // Sort the results by totalAbundance in descending order
            { $sort: { totalAbundance: -1 } }
        ];
    
        try {
            const result = await this.app.mongo.collection('site_ecocode_bundles').aggregate(pipeline).toArray();
            return result;
        } catch (error) {
            console.error("Failed to retrieve EcoCodes summary:", error);
            throw error;
        }
    }

    async getEcoCodesForSite(siteId, groupBySample = false) {
        if(this.app.useEcoCodeCaching) {
            let ecoCodeSiteBundle = await this.app.getObjectFromCache("site_ecocode_bundles", { site_id: parseInt(siteId) });
            if(ecoCodeSiteBundle) {
                return ecoCodeSiteBundle;
            }
        }

        let site = await this.app.getObjectFromCache("sites", { site_id: siteId });
        if(!site) {
            //If we don't have this site cached we give up on this request (at the moment!), but should perhaps be handled better in the future
            console.warn("Site "+siteId+" is not cached");
            return [];
        }
        
        //Filter out only abundance counting datasets
        let datasets = site.datasets.filter(dataset => {
            return dataset.method_id == 3; //method_id 3 is paleoentomology
        });

        //aggregation modes: "numberOfSpecies" (per eco code) or "countOfAbundace" (per eco code)
        
        /*
        {
            api_source: <version>,
            ecocodes: [
                {
                    ecocode_id,
                    species: 3,
                    abundance: 32 //This is, obviously, the aggregated abundance of all the specices that can be linked to this ecocode, obviously. Again, obviously (obviously).
                }
            ]
        }
        */

        let fetchPromises = [];
        let ecocodeBundles = [];
    
        datasets.forEach(dataset => {
            dataset.analysis_entities.forEach(ae => {
                ae.abundances.forEach(abundance => {
                    let promise = this.fetchEcoCodesForTaxon(abundance.taxon_id);
                    fetchPromises.push(promise);
                    promise.then(ecoCodes => {
                        if(ecoCodes === false) {
                            return {
                                status: "error",
                                msg: "Could not get eco codes for taxon "+abundance.taxon_id
                            };
                        }
                        ecoCodes.ecocodes.forEach(ecoCode => {
                            let bundleFound = false;
                            for(let bundleKey in ecocodeBundles) {
                                if(ecocodeBundles[bundleKey].ecocode.ecocode_definition_id == ecoCode.ecocode_definition_id) {
                                    bundleFound = true;
                                    if(!ecocodeBundles[bundleKey].taxa.includes(abundance.taxon_id)) {
                                        ecocodeBundles[bundleKey].taxa.push(abundance.taxon_id);
                                    }
                                    ecocodeBundles[bundleKey].abundance += abundance.abundance;
                                }
                            }
                            if(!bundleFound) {
                                ecocodeBundles.push({
                                    ecocode: ecoCode,
                                    taxa: [abundance.taxon_id],
                                    abundance: abundance.abundance
                                });
                            }
                        });
                        
                    });
                });
            });
        });
        

        await Promise.all(fetchPromises);

        let bundleObject = {
            api_source: this.app.appName+"-"+this.app.appVersion,
            site_id: siteId,
            site_age_summary: site.age_summary,
            ecocode_bundles: ecocodeBundles
        };

        if(this.app.useEcoCodeCaching) {
            this.app.saveObjectToCache("site_ecocode_bundles", { site_id: parseInt(siteId) }, bundleObject);
        }
        
        return bundleObject;
    }

    async getAggregatedEcoCodes() {
        const aggregationPipeline = [
          {
            $match: {
              "datasets.method_id": 3 // Filter abundance counting datasets (method_id 3 is paleoentomology)
            }
          },
          { $unwind: "$datasets" },
          { $unwind: "$datasets.analysis_entities" },
          { $unwind: "$datasets.analysis_entities.abundances" },
          {
            $group: {
              _id: "$datasets.analysis_entities.abundances.taxon_id",
              abundance: { $sum: "$datasets.analysis_entities.abundances.abundance" },
              ecocodes: {
                $push: {
                  ecocode_id: "$datasets.analysis_entities.abundances.ecocode_id",
                  species: 3, // Assuming species count is always 3
                  abundance: "$datasets.analysis_entities.abundances.abundance"
                }
              }
            }
          },
          {
            $group: {
              _id: null,
              ecocodes: { $push: "$ecocodes" }
            }
          },
          {
            $project: {
              _id: 0,
              api_source: "<version>", // Set your desired API source version here
              ecocodes: 1
            }
          }
        ];
      
        const aggregatedEcoCodes = await YourMongoCollection.aggregate(aggregationPipeline).toArray();
      
        return aggregatedEcoCodes[0]; // Assuming there's only one result (for all sites)
      }
      

    insertIfUnique(ecocodes, ecocode) {

        let found = false;
        for(let key in ecocodes) {
            if(ecocodes[key].name == ecocode.name) {
                found = true;
            }
        }
        if(!found) {
            ecocodes.push(ecocode);
        }
        return found;
    }
}

export default EcoCodes;
