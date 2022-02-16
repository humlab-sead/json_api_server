const fs = require('fs');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Pool } = require('pg');
const DendrochronologyModule = require('./Modules/DendrochronologyModule.class');
const AbundanceModule = require('./Modules/AbundanceModule.class');
const MeasuredValuesModule = require('./Modules/MeasuredValuesModule.class');

class SeadDataServer {
    constructor() {
        this.useSiteCaching = typeof(process.env.USE_SITE_CACHE) != "undefined" ? process.env.USE_SITE_CACHE : true;
        this.useStaticDbConnection = typeof(process.env.USE_SINGLE_PERSISTANT_DBCON) != "undefined" ? process.env.USE_SINGLE_PERSISTANT_DBCON : false;
        this.staticDbConnection = null;
        console.log("Starting up SEAD Data Server");
        this.expressApp = express();
        this.expressApp.use(cors());
        this.expressApp.use(bodyParser.json());
        this.setupDatabase().then(() => {
            this.setupEndpoints();

            this.modules = [];
            this.modules.push(new AbundanceModule(this));
            this.modules.push(new DendrochronologyModule(this));
            this.modules.push(new MeasuredValuesModule(this));

            this.run();
        });
    }

    setupEndpoints() {
        this.expressApp.get('/site/:siteId', async (req, res) => {
            let site = await this.getSite(req.params.siteId, true);
            res.send(JSON.stringify(site, null, 2));
        });

        this.expressApp.get('/sample/:sampleId', async (req, res) => {
            console.log(req.path);
            try {
                const pgClient = await this.getDbConnection();
                let data = await pgClient.query('SELECT * FROM tbl_analysis_entities WHERE physical_sample_id=$1', [req.params.sampleId]);
                let datasets = this.groupAnalysisEntitiesByDataset(data.rows);
                return res.send(JSON.stringify(datasets));
            }
            catch(error) {
                console.error("Couldn't connect to database");
                console.error(error);
                return res.send(JSON.stringify({
                    error: "Internal server error"
                }));
            }
        });

        this.expressApp.get('/preload/:flushSiteCache?', async (req, res) => {
            console.log(req.path);
            if(req.params.flushSiteCache) {
                await this.flushSiteCache();
            }
            await this.preloadAllSites();
            res.send("Preload complete");
        });

        this.expressApp.get('/flushSiteCache', async (req, res) => {
            console.log(req.path);
            await this.flushSiteCache();
            res.send("Flush of site cache complete");
        });

        this.expressApp.get('/dataset/:datasetId', async (req, res) => {
            let dataset = {
                dataset_id: req.params.datasetId
            };
        
            let data = await query('SELECT * FROM tbl_datasets WHERE dataset_id=$1', [req.params.datasetId]);
            dataset = data.rows[0];
        
            let method = await query('SELECT * FROM tbl_methods WHERE method_id=$1', [data.rows[0].method_id]);
            dataset.method = method.rows[0];
        
            let ae = await query('SELECT * FROM tbl_analysis_entities WHERE dataset_id=$1', [dataset.dataset_id]);
            dataset.analysis_entities = ae.rows;
            
        
            if(dataset.biblio_id != null) {
                dataset.biblio = await fetchBiblioByDatasetId(dataset.dataset_id);
            }
        
            //If dataset belongs to certain methods, it might include dating data, so fetch it
            if(datingMethodGroups.includes(dataset.method.method_group_id)) {
                if(dataset.analysis_entities.length > 0) {
                    await fetchDatingToPeriodData(dataset.analysis_entities);
                }
            }
            
            await fetchPhysicalSamplesByAnalysisEntities(dataset.analysis_entities);
        
            res.send(dataset);
        });
    }

    async flushSiteCache() {
        console.log("Flushing site cache");
        let files = fs.readdirSync("site_cache");
        for(let key in files) {
            fs.unlinkSync("site_cache/"+files[key]);
        }
    }

    async preloadAllSites() {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        console.time("Preload of sites complete");

        console.log("Preloading sites");

        let siteData = await pgClient.query('SELECT * FROM tbl_sites ORDER BY site_id');
        this.releaseDbConnection(pgClient);

        console.log("Will fetch site data for "+siteData.rows.length+" sites");

        let siteIds = [];
        for(let key in siteData.rows) {
            siteIds.push(siteData.rows[key].site_id);
        }

        let maxConcurrentFetches = parseInt(process.env.MAX_CONCURRENT_FETCHES);
        maxConcurrentFetches = isNaN(maxConcurrentFetches) == false ? maxConcurrentFetches : 10;

        let pendingFetches = 0;

        await new Promise((resolve, reject) => {
            const fetchCheckInterval = setInterval(() => {
                if(siteIds.length > 0 && pendingFetches < maxConcurrentFetches) {
                    pendingFetches++;
                    let siteId = siteIds.shift();
                    //console.time("Site "+siteId+" fetched");
                    console.log("Fetching site", siteId);
                    this.getSite(siteId, false).then(() => {
                        //console.timeEnd("Site "+siteId+" fetched");
                        pendingFetches--;
                    });
                }
                if(siteIds.length == 0) {
                    clearInterval(fetchCheckInterval);
                    console.timeEnd("Preload of sites complete");
                    resolve();
                }
            }, 100);
        });

        console.timeEnd("Preload of sites complete");
    }

    async getSite(siteId, verbose = true, fetchMethodSpecificData = true) {
        if(verbose) console.log("Request for site", siteId);
        let site = null;
        if(this.useSiteCaching) {
            site = this.getSiteFromCache(siteId);
            if(site) {
                return site;
            }
        }

        if(verbose) console.time("Done fetching site "+siteId);

        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }
        if(verbose) console.time("Fetched basic site data for site "+siteId);
        let siteData = await pgClient.query('SELECT * FROM tbl_sites WHERE site_id=$1', [siteId]);
        site = siteData.rows[0];
        if(verbose) console.timeEnd("Fetched basic site data for site "+siteId);
        this.releaseDbConnection(pgClient);

        if(verbose) console.time("Fetched sample groups for site "+siteId);
        this.fetchSampleGroups(site);
        if(verbose) console.timeEnd("Fetched sample groups for site "+siteId);

        if(verbose) console.time("Fetched site location data for site "+siteId);
        await this.fetchSiteLocation(site);
        if(verbose) console.timeEnd("Fetched site location data for site "+siteId);

        if(verbose) console.time("Fetched sample group sampling methods for site "+siteId);
        await this.fetchSampleGroupsSamplingMethods(site);
        if(verbose) console.timeEnd("Fetched sample group sampling methods for site "+siteId);
        
        if(verbose) console.time("Fetched physical samples for site "+siteId);
        await this.fetchPhysicalSamples(site);
        if(verbose) console.timeEnd("Fetched physical samples for site "+siteId);

        if(verbose) console.time("Fetched analysis entities for site "+siteId);
        await this.fetchAnalysisEntities(site);
        if(verbose) console.timeEnd("Fetched analysis entities for site "+siteId);
        
        if(verbose) console.time("Fetched datasets for site "+siteId);
        await this.fetchDatasets(site);
        if(verbose) console.timeEnd("Fetched datasets for site "+siteId);
        
        
        if(verbose) console.time("Fetched analysis methods for site "+siteId);
        await this.fetchAnalysisMethods(site);
        if(verbose) console.timeEnd("Fetched analysis methods for site "+siteId);

        if(fetchMethodSpecificData) {
            if(verbose) console.time("Fetched method specific data for site "+siteId);
            await this.fetchMethodSpecificData(site);
            if(verbose) console.timeEnd("Fetched method specific data for site "+siteId);
        }
        
        if(verbose) console.timeEnd("Done fetching site");

        if(this.useSiteCaching) {
            //Store in cache
            this.saveSiteToCache(site);
        }

        return site;
    }

    async fetchSampleGroups(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sampleGroups = await pgClient.query('SELECT * FROM tbl_sample_groups WHERE site_id=$1', [site.site_id]);
        site.sample_groups = sampleGroups.rows;

        for(let key in site.sample_groups) {
            let sampleGroupsCoords = await pgClient.query('SELECT * FROM tbl_sample_group_coordinates WHERE sample_group_id=$1', [site.sample_groups[key].sample_group_id]);
            site.sample_groups[key].coordinates = sampleGroupsCoords.rows;
        }
        
        for(let key in site.sample_groups) {
            let sampleGroupsDesc = await pgClient.query('SELECT * FROM tbl_sample_group_descriptions WHERE sample_group_id=$1', [site.sample_groups[key].sample_group_id]);
            site.sample_groups[key].descriptions = sampleGroupsDesc.rows;

            for(let key2 in site.sample_groups[key].descriptions) {
                let sql = `
                SELECT *
                FROM tbl_sample_group_descriptions 
                LEFT JOIN tbl_sample_group_description_types ON tbl_sample_group_descriptions.sample_group_description_type_id = tbl_sample_group_description_types.sample_group_description_type_id
                WHERE sample_group_description_id=$1
                `;

                let sampleGroupsDescType = await pgClient.query(sql, [site.sample_groups[key].descriptions[key2].sample_group_description_id]);
                site.sample_groups[key].descriptions[key2].description_type = sampleGroupsDescType.rows;
            }

            let sampleGroupSamplingContext = await pgClient.query('SELECT * FROM tbl_sample_group_sampling_contexts WHERE sampling_context_id=$1', [site.sample_groups[key].sampling_context_id]);
            site.sample_groups[key].sampling_context = sampleGroupSamplingContext.rows;

            let sampleGroupNotes = await pgClient.query('SELECT * FROM tbl_sample_group_notes WHERE sample_group_id=$1', [site.sample_groups[key].sample_group_id]);
            site.sample_groups[key].notes = sampleGroupNotes.rows;
        }

        
        
        this.releaseDbConnection(pgClient);
        
        return site;
    }

    async fetchSiteLocation(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }
        let sql = `
        SELECT * FROM tbl_site_locations
        LEFT JOIN tbl_locations ON tbl_site_locations.location_id = tbl_locations.location_id
        WHERE site_id=$1
        `;
        let siteLocData = await pgClient.query(sql, [site.site_id]);
        site.location = siteLocData.rows;
        
        this.releaseDbConnection(pgClient);
        
        return site;
    }

    async fetchMethodSpecificData(site, verbose = true) {
        let fetchPromises = [];
        
        for(let key in this.modules) {
            let module = this.modules[key];
            //if(verbose) console.time("Fetched method "+module.name);
            let promise = module.fetchSiteData(site);
            fetchPromises.push(promise);
            promise.then(() => {
                //if(verbose) console.timeEnd("Fetched method "+module.name);
            });
        }

        return Promise.all(fetchPromises);
    }

    async fetchAnalysisMethods(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let methodIds = [];
        site.datasets.forEach(dataset => {
            if(methodIds.indexOf(dataset.method_id) == -1) {
                methodIds.push(dataset.method_id);
            }
        });

        let queryPromises = [];
        let methods = [];
        methodIds.forEach(methodId => {
            let promise = pgClient.query('SELECT * FROM tbl_methods WHERE method_id=$1', [methodId]).then(method => {
                methods.push(method.rows[0]);
            });
            queryPromises.push(promise);
        });

        await Promise.all(queryPromises).then(() => {
            this.releaseDbConnection(pgClient);
        });
        
        site.analysis_methods = methods;

        return site;
    }

    async fetchDatasets(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        //Get the unqiue dataset_ids
        let datasetIds = [];
        site.sample_groups.forEach(sampleGroup => {
            sampleGroup.physical_samples.forEach(physicalSample => {
                physicalSample.analysis_entities.forEach(analysisEntity => {
                    if(datasetIds.indexOf(analysisEntity.dataset_id) == -1) {
                        datasetIds.push(analysisEntity.dataset_id);
                    }
                })
            })
        });

        let queryPromises = [];
        let datasets = [];
        datasetIds.forEach(datasetId => {
            let promise = pgClient.query('SELECT * FROM tbl_datasets WHERE dataset_id=$1', [datasetId]).then(dataset => {
                datasets.push(dataset.rows[0]);
            });
            queryPromises.push(promise);
        });

        await Promise.all(queryPromises).then(() => {
            this.releaseDbConnection(pgClient);
        });
        
        site.datasets = datasets;

        return site;
    }

    async fetchSampleGroupsSamplingMethods(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let queryPromises = [];
        site.sample_groups.forEach(sampleGroup => {
            
            let promise = pgClient.query('SELECT * FROM tbl_methods WHERE method_id=$1', [sampleGroup.method_id]).then(method => {
                sampleGroup.sampling_method = method.rows[0];
            });
            queryPromises.push(promise);
        });

        await Promise.all(queryPromises).then(() => {
            this.releaseDbConnection(pgClient);
        });

        return site;
    }

    async fetchAnalysisEntities(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let queryPromises = [];
        site.sample_groups.forEach(sampleGroup => {
            sampleGroup.physical_samples.forEach(physicalSample => {
                let promise = pgClient.query('SELECT * FROM tbl_analysis_entities WHERE physical_sample_id=$1', [physicalSample.physical_sample_id]).then(analysisEntities => {
                    physicalSample.analysis_entities = analysisEntities.rows;
                });
                queryPromises.push(promise);
            });
        });

        await Promise.all(queryPromises).then(() => {
            this.releaseDbConnection(pgClient);
        });

        return site;
    }

    async fetchPhysicalSamples(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }
        
        for(let key in site.sample_groups) {
            let sampleGroup = site.sample_groups[key];
            let physicalSamples = await pgClient.query('SELECT * FROM tbl_physical_samples WHERE sample_group_id=$1', [sampleGroup.sample_group_id]);
            sampleGroup.physical_samples = physicalSamples.rows;

            for(let sampleKey in sampleGroup.physical_samples) {
                let sample = sampleGroup.physical_samples[sampleKey];
                
                let sql = `SELECT * 
                FROM tbl_physical_sample_features
                LEFT JOIN tbl_features ON tbl_physical_sample_features.feature_id = tbl_features.feature_id
                LEFT JOIN tbl_feature_types ON tbl_features.feature_type_id = tbl_feature_types.feature_type_id
                WHERE physical_sample_id=$1
                `;
                let sampleFeatures = await pgClient.query(sql, [sample.physical_sample_id]);
                sample.features = sampleFeatures.rows;
            
                sql = `SELECT * 
                FROM tbl_sample_descriptions
                LEFT JOIN tbl_sample_description_types ON tbl_sample_descriptions.sample_description_type_id = tbl_sample_description_types.sample_description_type_id
                WHERE tbl_sample_descriptions.physical_sample_id=$1
                `;
                let sampleDescriptions = await pgClient.query(sql, [sample.physical_sample_id]);
                sample.descriptions = sampleDescriptions.rows;

                sql = `SELECT *
                FROM tbl_sample_locations
                LEFT JOIN tbl_sample_location_types ON tbl_sample_locations.sample_location_type_id = tbl_sample_location_types.sample_location_type_id
                WHERE tbl_sample_locations.physical_sample_id=$1
                `;
                let sampleLocations = await pgClient.query(sql, [sample.physical_sample_id]);
                sample.locations = sampleLocations.rows;

                sql = `SELECT *
                FROM tbl_sample_alt_refs
                LEFT JOIN tbl_alt_ref_types ON tbl_sample_alt_refs.alt_ref_type_id = tbl_alt_ref_types.alt_ref_type_id
                WHERE tbl_sample_alt_refs.physical_sample_id=$1
                `;
                let altRefs = await pgClient.query(sql, [sample.physical_sample_id]);
                sample.alt_refs = altRefs.rows;

                sql = `SELECT *
                FROM tbl_sample_dimensions
                LEFT JOIN tbl_methods ON tbl_sample_dimensions.method_id = tbl_methods.method_id
                WHERE tbl_sample_dimensions.physical_sample_id=$1
                `;
                let sampleDimensions = await pgClient.query(sql, [sample.physical_sample_id]);
                sample.dimensions = sampleDimensions.rows;
                
            }

        }
        
        this.releaseDbConnection(pgClient);

        return site;
    }

    getSiteFromCache(siteId) {
        let result = false;
        try {
            result = fs.readFileSync("site_cache/site_"+siteId+".json");
        }
        catch(error) {
            return false;
        }
        if(result) {
            return JSON.parse(result);
        }
        return false;
    }

    async saveSiteToCache(site) {
        return fs.writeFileSync("site_cache/site_"+site.site_id+".json", JSON.stringify(site, null, 2));
    }


    groupAnalysisEntitiesByDataset(analysisEntities) {
        let datasets = [];

        let datasetsIds = analysisEntities.map((ae) => {
            return ae.dataset_id;
        });

        datasetsIds = datasetsIds.filter((value, index, self) => {
            return self.indexOf(value) === index;
        });

        datasetsIds.forEach((datasetId) => {
            datasets.push({
                datasetId: datasetId,
                analysisEntities: []
            });
        })

        for(let key in analysisEntities) {

            for(let dsKey in datasets) {
                if(analysisEntities[key].dataset_id == datasets[dsKey].datasetId) {
                    datasets[dsKey].analysisEntities.push(analysisEntities[key]);
                }
            }
        }

        return datasets;
    }

    async setupDatabase() {
        try {
            this.pgPool = new Pool({
                user: process.env.POSTGRES_USER,
                host: process.env.POSTGRES_HOST,
                database: process.env.POSTGRES_DATABASE,
                password:process.env.POSTGRES_PASS,
                port: process.env.POSTGRES_PORT,
                max: 100,
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 60000,
            });

            if(this.useStaticDbConnection) {
                console.log("Setting up static db connection");
                this.staticDbConnection = await this.pgPool.connect();
                console.log("Database ready");
            }

            return true;
        }
        catch (err) {
            console.error(err);
            return false;
        }
    };

    async getDbConnection() {
        if(this.useStaticDbConnection) {
            return this.staticDbConnection;
        }
        else {
            let dbcon = false;
            try {
                dbcon = await this.pgPool.connect();
            }
            catch(error) {
                console.error("Couldn't connect to database");
                console.error(error);
                return false;
            }
            return dbcon;
        }
    }

    async releaseDbConnection(dbConn) {
        if(this.useStaticDbConnection) {
            //Never release if using static conn
            return true;
        }
        else {
            return dbConn.release();
        }
    }

    async query(sql, params = []) {
        let pgClient = await pgPool.connect();
        let resultData = await pgClient.query(sql, params);
        pgClient.release();
        return resultData;
    }

    run() {
        this.server = this.expressApp.listen(process.env.API_PORT, () =>
            console.log("Webserver started, listening at port", process.env.API_PORT),
        );
    }


    shutdown() {
        this.pgPool.end();
        this.server.close(() => {
            console.log('Server shutdown');
            process.exit(0);
        });
    }
}

module.exports = SeadDataServer;