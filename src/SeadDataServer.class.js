const fs = require('fs');
const { unlink } = require('fs/promises');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Pool } = require('pg');
const crypto = require('crypto');
const { MongoClient } = require('mongodb');
const DendrochronologyModule = require('./Modules/DendrochronologyModule.class');
const AbundanceModule = require('./Modules/AbundanceModule.class');
const MeasuredValuesModule = require('./Modules/MeasuredValuesModule.class');


const appName = "seaddataserver";
const appVersion = "1.5.1";

class SeadDataServer {
    constructor() {
        this.siteCacheStorage = typeof(process.env.SITE_CACHE_STORAGE) != "undefined" ? process.env.SITE_CACHE_STORAGE : "file";
        this.useSiteCaching = typeof(process.env.USE_SITE_CACHE) != "undefined" ? process.env.USE_SITE_CACHE == "true" : true;
        this.useStaticDbConnection = typeof(process.env.USE_SINGLE_PERSISTANT_DBCON) != "undefined" ? process.env.USE_SINGLE_PERSISTANT_DBCON == "true" : false;
        this.staticDbConnection = null;
        console.log("Starting up SEAD Data Server "+appVersion);
        if(this.useSiteCaching) {
            console.log("Site cache is enabled")
            console.log("Using "+this.siteCacheStorage+" for site cache storage");
        }
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

        this.setupMongoDb().then(() => {
            console.log('Connected to MongoDB');
        });
    }

    async setupMongoDb() {
        const mongoUser = encodeURIComponent(process.env.MONGO_USER);
        const mongoPass = encodeURIComponent(process.env.MONGO_PASS);
        this.mongoClient = new MongoClient('mongodb://'+mongoUser+':'+mongoPass+'@'+process.env.MONGO_HOST+':27017/');
        await this.mongoClient.connect();
        let db = this.mongoClient.db(process.env.MONGO_DB);
        this.mongoCollection = db.collection('sites');
    }

    getHashFromString(string) {
        let sha = crypto.createHash('sha1');
        let hashKey = string.toString();
        sha.update(hashKey);
        return sha.digest('hex');
    }

    setupEndpoints() {
        this.expressApp.get('/version', async (req, res) => {
            let versionInfo = {
                app: "Sead Data Server",
                version: appVersion
            }
            res.send(JSON.stringify(versionInfo, null, 2));
        });

        this.expressApp.get('/site/:siteId', async (req, res) => {
            let site = await this.getSite(req.params.siteId, true);
            res.header("Content-type", "application/json");
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
                    console.time("Fetched site "+siteId);
                    //console.log("Fetching site", siteId);
                    this.getSite(siteId, false).then(() => {
                        console.timeEnd("Fetched site "+siteId);
                        pendingFetches--;
                    });
                }
                if(siteIds.length == 0) {
                    clearInterval(fetchCheckInterval);
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
            site = await this.getSiteFromCache(siteId);
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
        site.data_groups = [];
        site.api_source = appName+"-"+appVersion;

        if(verbose) console.timeEnd("Fetched basic site data for site "+siteId);
        this.releaseDbConnection(pgClient);

        if(verbose) console.time("Fetched biblio for site "+siteId);
        await this.fetchSiteBiblio(site);
        if(verbose) console.timeEnd("Fetched biblio for site "+siteId);

        if(verbose) console.time("Fetched sample groups for site "+siteId);
        await this.fetchSampleGroups(site);
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

        if(verbose) console.timeEnd("Done fetching site "+siteId);

        if(verbose) console.time("Done processing data for site "+siteId);
        this.postProcessSiteData(site);
        if(verbose) console.timeEnd("Done processing data for site "+siteId);

        if(this.useSiteCaching) {
            //Store in cache
            this.saveSiteToCache(site);
        }

        return site;
    }

    getAnalysisMethodByMethodId(site, method_id) {
        for(let key in site.analysis_methods) {
            if(site.analysis_methods[key].method_id == method_id) {
                return site.analysis_methods[key];
            }
        }
    }

    getSampleNameBySampleId(site, physical_sample_id) {
        for(let sgKey in site.sample_groups) {
            for(let sampleKey in site.sample_groups[sgKey].physical_samples) {
                if(site.sample_groups[sgKey].physical_samples[sampleKey].physical_sample_id == physical_sample_id) {
                    return site.sample_groups[sgKey].physical_samples[sampleKey].sample_name;
                }
            }
        }
        return false;
    }

    postProcessSiteData(site) {
        //Re-arrange things so that analysis_entities are connected to the datasets instead of the samples
        let analysisEntities = [];
        for(let sgKey in site.sample_groups) {
            let sampleGroup = site.sample_groups[sgKey];
            for(let sampleKey in sampleGroup.physical_samples) {
                let sample = sampleGroup.physical_samples[sampleKey];
                for(let aeKey in sample.analysis_entities) {
                    analysisEntities.push(sample.analysis_entities[aeKey]);
                }
            }
        }
        let datasets = this.groupAnalysisEntitiesByDataset(analysisEntities);
        site.datasets.forEach(dataset => {
            datasets.forEach(datasetAes => {
                if(dataset.dataset_id == datasetAes.dataset_id) {
                    dataset.analysis_entities = datasetAes.analysis_entities;
                }
            });
        });

        //Unlink analysis_entities from samples
        site.sample_groups.forEach(sg => {
            sg.physical_samples.forEach(ps => {
                delete ps.analysis_entities;
            })
        })

        //Here we give each module a chance to modify the data structure now that everything is fetched
        for(let key in this.modules) {
            let module = this.modules[key];
            module.postProcessSiteData(site);
        }
        return site;
    }

    async fetchSiteBiblio(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }
        let sql = `
        SELECT * FROM tbl_site_references
        LEFT JOIN tbl_biblio ON tbl_site_references.biblio_id = tbl_biblio.biblio_id
        WHERE site_id=$1
        `;
        let siteBiblioRef = await pgClient.query(sql, [site.site_id]);
        site.biblio = siteBiblioRef.rows;
        
        this.releaseDbConnection(pgClient);
        
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
            let sql = `
            SELECT 
            tbl_sample_group_references.biblio_id,
            tbl_biblio.doi,
            tbl_biblio.isbn,
            tbl_biblio.notes,
            tbl_biblio.title,
            tbl_biblio.year,
            tbl_biblio.authors,
            tbl_biblio.full_reference,
            tbl_biblio.url
            FROM tbl_sample_group_references
            INNER JOIN tbl_biblio ON tbl_biblio.biblio_id = tbl_sample_group_references.biblio_id
            WHERE tbl_sample_group_references.sample_group_id=$1
            `;

            let sampleGroupsRefs = await pgClient.query(sql, [site.sample_groups[key].sample_group_id]);
            site.sample_groups[key].biblio = sampleGroupsRefs.rows;
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
        SELECT 
        tbl_locations.location_id,
        tbl_locations.location_name,
        tbl_locations.default_lat_dd,
        tbl_locations.default_long_dd,
        tbl_locations.location_type_id,
        tbl_location_types.location_type,
        tbl_location_types.description AS location_description
        FROM tbl_site_locations
        LEFT JOIN tbl_locations ON tbl_site_locations.location_id = tbl_locations.location_id
        LEFT JOIN tbl_location_types ON tbl_locations.location_type_id = tbl_location_types.location_type_id
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

            //This should get "data_groups", a data_group has data structured in tabular form, like:
            /*
            data_group.id = "dataset 1"; //id/context can be whatever makes sense for the given data type. For abundance and measured_value it's the dataset,
            but dendro has a separate dataset for each of what we here would call a datapoint, so there it makes more sense to let the id/context be equal to the sample id/name instead
            data_group.data_points[
                {
                    label: "a",
                    value: 0
                },
                {
                    label: "b",
                    value: 1
                }
            ]
            */
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
            if(typeof(dataset) == "undefined") {
                console.log("Dataset in site "+site.site_id+" was undefined");
            }
            if(typeof(dataset.method_id) == "undefined") {
                console.log("Method id in dataset "+dataset.dataset_id+" was undefined");
                console.log(dataset);
            }
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
        let sql = `
        SELECT
        tbl_datasets.*,
        tbl_methods.method_group_id
        FROM tbl_datasets 
        LEFT JOIN tbl_methods ON tbl_datasets.method_id = tbl_methods.method_id
        WHERE dataset_id=$1
        `;
        datasetIds.forEach(datasetId => {
            let promise = pgClient.query(sql, [datasetId]).then(dataset => {
                if(dataset.rows.length > 0) {
                    datasets.push(dataset.rows[0]);
                }
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
            let sql = `
            SELECT 
            tbl_physical_samples.*,
            tbl_sample_types.type_name AS sample_type_name,
            tbl_sample_types.description AS sample_type_description
            FROM tbl_physical_samples
            LEFT JOIN tbl_sample_types ON tbl_physical_samples.sample_type_id = tbl_sample_types.sample_type_id
            WHERE sample_group_id=$1
            `;
            let physicalSamples = await pgClient.query(sql, [sampleGroup.sample_group_id]);
            sampleGroup.physical_samples = physicalSamples.rows;

            for(let sampleKey in sampleGroup.physical_samples) {
                let sample = sampleGroup.physical_samples[sampleKey];
                
                sql = `SELECT * 
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

    async getSiteFromCache(siteId) {
        let site = null;
        if(this.siteCacheStorage == "mongo") {
            let siteFindResult = await this.mongoCollection.find({
                site_id: parseInt(siteId)
            }).toArray();
    
            site = siteFindResult[0];
        }

        if(this.siteCacheStorage == "file") {
            try {
                site = fs.readFileSync("site_cache/site_"+siteId+".json");
                if(site) {
                    site = JSON.parse(site);
                }
            }
            catch(error) {
                //Nope, no such file
            }
        }

        //Check that this cached version of the site was generated by the same server version as we are running
        //If not, it will be re-generated using the (hopefully) newer version
        if(site && site.api_source == appName+"-"+appVersion) {
            return site;
        }
        return false;
    }

    async saveSiteToCache(site) {
        if(this.siteCacheStorage == "mongo") {
            await this.mongoCollection.deleteOne({ site_id: parseInt(site.site_id) });
            this.mongoCollection.insertOne(site);
        }
        if(this.siteCacheStorage == "file") {
            fs.writeFileSync("site_cache/site_"+site.site_id+".json", JSON.stringify(site, null, 2));
        }
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
                dataset_id: datasetId,
                analysis_entities: []
            });
        })

        for(let key in analysisEntities) {
            for(let dsKey in datasets) {
                if(analysisEntities[key].dataset_id == datasets[dsKey].dataset_id) {
                    datasets[dsKey].analysis_entities.push(analysisEntities[key]);
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