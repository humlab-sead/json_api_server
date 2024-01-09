const fs = require('fs');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Pool } = require('pg');
const crypto = require('crypto');
const zip = new require('node-zip')();
const { MongoClient } = require('mongodb');
const DendrochronologyModule = require('./DataFetchingModules/DendrochronologyModule.class');
const AbundanceModule = require('./DataFetchingModules/AbundanceModule.class');
const MeasuredValuesModule = require('./DataFetchingModules/MeasuredValuesModule.class');
const CeramicsModule = require('./DataFetchingModules/CeramicsModule.class');
const DatingModule = require('./DataFetchingModules/DatingModule.class');

const EcoCodes = require("./EndpointModules/EcoCodes.class");
const Chronology = require("./EndpointModules/Chronology.class");
const Taxa = require("./EndpointModules/Taxa.class");
const Graphs = require("./EndpointModules/Graphs.class");
const res = require('express/lib/response');

const appName = "sead-json-api-server";
const appVersion = "1.31.2";

class SeadJsonApiServer {
    constructor() {
        this.appName = appName;
        this.appVersion = appVersion;
        this.cacheStorageMethod = typeof(process.env.CACHE_STORAGE_METHOD) != "undefined" ? process.env.CACHE_STORAGE_METHOD : "file";
        this.useSiteCaching = typeof(process.env.USE_SITE_CACHE) != "undefined" ? process.env.USE_SITE_CACHE == "true" : true;
        this.useQueryCaching = typeof(process.env.USE_QUERY_CACHE) != "undefined" ? process.env.USE_QUERY_CACHE == "true" : true;
        this.useTaxaCaching = typeof(process.env.USE_TAXA_CACHE) != "undefined" ? process.env.USE_TAXA_CACHE == "true" : true;
        this.useTaxaSummaryCacheing = typeof(process.env.USE_TAXA_SUMMARY_CACHE) != "undefined" ? process.env.USE_TAXA_SUMMARY_CACHE == "true" : true;
        this.useEcoCodeCaching = typeof(process.env.USE_ECOCODE_CACHE) != "undefined" ? process.env.USE_ECOCODE_CACHE == "true" : true;
        this.useStaticDbConnection = typeof(process.env.USE_SINGLE_PERSISTENT_DBCON) != "undefined" ? process.env.USE_SINGLE_PERSISTENT_DBCON == "true" : false;
        this.acceptCacheVersionMismatch = typeof(process.env.ACCEPT_CACHE_VERSION_MISMATCH) != "undefined" ? process.env.ACCEPT_CACHE_VERSION_MISMATCH == "true" : false;
        this.allCachingDisabled = typeof(process.env.FORCE_DISABLE_ALL_CACHES) != "undefined" ? process.env.FORCE_DISABLE_ALL_CACHES == "true" : false;
        this.maxConcurrentFetches = parseInt(process.env.MAX_CONCURRENT_FETCHES) ? parseInt(process.env.MAX_CONCURRENT_FETCHES) : process.env.MAX_CONCURRENT_FETCHES = 1;
        
        this.staticDbConnection = null;
        console.log("Starting up SEAD Data Server "+appVersion);
        if(this.useSiteCaching) {
            console.log("Site cache is enabled")
            console.log("Using "+this.cacheStorageMethod+" for site cache storage");
        }
        this.expressApp = express();
        this.expressApp.use(cors());
        this.expressApp.use(bodyParser.json());
        this.setupDatabase().then(() => {
            this.setupEndpoints();

            //These are the data fetching modules
            //they have some requirements on them regarding methods they need to implement
            this.dataFetchingModules = [];
            this.dataFetchingModules.push(new AbundanceModule(this));
            this.dataFetchingModules.push(new DendrochronologyModule(this));
            this.dataFetchingModules.push(new MeasuredValuesModule(this));
            this.dataFetchingModules.push(new CeramicsModule(this));
            const datingModule = new DatingModule(this);
            this.dataFetchingModules.push(datingModule);

            //These are the endpoint modules, they have no implementation requirements
            this.ecoCodes = new EcoCodes(this);
            this.chronology = new Chronology(this, datingModule);
            this.taxa = new Taxa(this);
            this.graphs = new Graphs(this);

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
        this.mongo = this.mongoClient.db(process.env.MONGO_DB);
    }

    getHashFromString(string) {
        let sha = crypto.createHash('sha1');
        let hashKey = string.toString();
        sha.update(hashKey);
        return sha.digest('hex');
    }

    setupEndpoints() {
        this.expressApp.all('*', (req, res, next) => {
            console.log("Request: "+req.method+" "+req.path+" initiating");
            console.time("Request: "+req.method+" "+req.path+" completed");
            next();
            res.addListener("close", () => {
                console.timeEnd("Request: "+req.method+" "+req.path+" completed");
            });
        });

        this.expressApp.get('/version', async (req, res) => {
            let versionInfo = {
                app: "SEAD JSON API Server",
                version: appVersion
            }
            res.send(JSON.stringify(versionInfo, null, 2));
        });

        /*
        this.expressApp.get('/test', async (req, res) => {
            console.time("All sites fetched")
            for(let i = 0; i < 1000; i++) {
                console.time("Fetch of site "+i);
                await this.getObjectFromCache("sites", { site_id: i });
                console.timeEnd("Fetch of site "+i);
            }
            console.timeEnd("All sites fetched");
            res.header("Content-type", "application/json");
            res.end();
        });
        */

        this.expressApp.get('/search/site/:search/:value', async (req, res) => {
            let siteIds = await this.searchSites(req.params.search, req.params.value);
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(siteIds, null, 2));
        });

        this.expressApp.get('/site/:siteId', async (req, res) => {
            let site = await this.getSite(req.params.siteId, true);
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(site, null, 2));
        });

        this.expressApp.post('/export/sites', async (req, res) => {
            let siteIds = req.body;
            console.log(siteIds);
            if(typeof siteIds != "object") {
                res.status(400);
                res.send("Bad input - should be an array of site IDs");
                return;
            }
            siteIds.forEach(siteId => {
                if(!parseInt(siteId)) {
                    res.status(400);
                    res.send("Bad input - should be an array of site IDs");
                    return;
                }
            })
            let data = await this.exportSites(siteIds);
            let jsonData = JSON.stringify(data, null, 2);

            zip.file('sites_export.json', jsonData);
            let compressed = zip.generate({base64:false,compression:'DEFLATE'});

            res.writeHead(200, {
                'Content-Type': "application/zip",
                'Content-disposition': 'attachment;filename=' + "sites_export.zip",
                'Content-Length': compressed.length
            });
            res.end(Buffer.from(compressed, 'binary'));
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
                console.error("Couldn't connect to postgres database");
                console.error(error);
                return res.send(JSON.stringify({
                    error: "Internal server error"
                }));
            }
        });

        this.expressApp.get('/preload/sites/:flushCache?', async (req, res) => {
            console.log(req.path);
            if(req.params.flushCache) {
                await this.flushSiteCache();
            }
            await this.preloadAllSites();
            res.end("Preload of sites complete");
        });

        this.expressApp.get('/preload/taxa/:flushCache?', async (req, res) => {
            console.log(req.path);
            if(req.params.flushCache) {
                await this.flushTaxaCache();
            }
            await this.preloadAllTaxa();
            res.send("Preload of taxa complete");
        });

        this.expressApp.get('/preload/ecocodes/:flushCache?', async (req, res) => {
            console.log(req.path);
            if(req.params.flushCache) {
                await this.ecoCodes.flushEcocodeCache();
            }
            await this.ecoCodes.preloadAllSiteEcocodes(); 
            res.send("Preload of ecocodes complete");
        });

        this.expressApp.get('/flush/graphcache', async (req, res) => {
            console.log(req.path);
            await this.flushGraphCache();
            res.send("Flushed graph cache");
        });

        this.expressApp.get('/preload/all', async (req, res) => {
            console.log(req.path);
            console.time("Preload of all data complete");
            await this.preloadAllSites(); // collection: sites
            await this.preloadAllTaxa(); // collection: taxa
            await this.ecoCodes.preloadAllSiteEcocodes(); 
            console.timeEnd("Preload of all data complete");
            res.send("Preload of all data complete");
        });

        this.expressApp.get('/flushSiteCache', async (req, res) => {
            console.log(req.path);
            await this.flushSiteCache();
            res.send("Flush of site cache complete");
        });

        this.expressApp.get('/taxon/:taxonId', async (req, res) => {
            let taxonId = parseInt(req.params.taxonId);
            if(!taxonId) {
                console.log(req.path, "Bad request");
                res.status(400);
                res.send("Bad request");
                return;
            }

            let taxon = await this.getTaxon(taxonId);

            res.header("Content-type", "application/json");
            res.send(JSON.stringify(taxon, null, 2));
        });

        this.expressApp.get('/search/taxon/:attribute/:value', async (req, res) => {
            let taxonIds = await this.searchTaxa(req.params.attribute, req.params.value);
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(taxonIds, null, 2));
        });

        this.expressApp.get('/getsiteswithsamplecoordinates', async (req, res) => {
            let sites = await this.getSitesWithSampleCoordinates();
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(sites, null, 2));
        });
    }

    async exportSites(siteIds) {
        let promises = [];
        siteIds.forEach(siteId => {
            promises.push(this.getSite(siteId));
        });
        
        return await Promise.all(promises);
    }

    async getTaxon(taxonId, verbose = true) {
        if(verbose) console.log("Request for taxon", taxonId);
        let taxon = null;
        let identifierObject = { taxon_id: taxonId };
        if(this.useTaxaCaching) {
            taxon = await this.getObjectFromCache("taxa", identifierObject);
            if(taxon) {
                return taxon;
            }
        }
        
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let rows = await this.fetchFromTable("tbl_taxa_tree_master", "taxon_id", taxonId);
        taxon = rows[0];

        taxon.api_source = appName+"-"+appVersion;

        let generaRes = await this.fetchFromTable("tbl_taxa_tree_genera", "genus_id", taxon.genus_id);
        taxon.genus = generaRes[0];
        
        let familyRes = await this.fetchFromTable("tbl_taxa_tree_families", "family_id", taxon.genus.family_id);
        taxon.family = familyRes[0];

        let orderRes = await this.fetchFromTable("tbl_taxa_tree_orders", "order_id", taxon.family.order_id);
        taxon.order = orderRes[0];


        let ecocodeSql = `
        SELECT 
        tbl_ecocodes.*,
        tbl_ecocode_definitions.ecocode_definition_id,
        tbl_ecocode_definitions.abbreviation AS ecocode_abbreviation,
        tbl_ecocode_definitions.name AS ecocode_name,
        tbl_ecocode_definitions.definition AS ecocode_definition,
        tbl_ecocode_definitions.notes AS ecocode_notes,
        tbl_ecocode_systems.ecocode_system_id,
        tbl_ecocode_systems.definition as system_definition,
        tbl_ecocode_systems.name as system_name,
        tbl_ecocode_systems.notes as system_notes
        FROM tbl_ecocodes
        JOIN tbl_ecocode_definitions ON tbl_ecocode_definitions.ecocode_definition_id=tbl_ecocodes.ecocode_definition_id
        JOIN tbl_ecocode_groups ON tbl_ecocode_groups.ecocode_group_id=tbl_ecocode_definitions.ecocode_group_id
        JOIN tbl_ecocode_systems ON tbl_ecocode_systems.ecocode_system_id=tbl_ecocode_groups.ecocode_system_id
        WHERE taxon_id=$1`;
        let ecocodeRes = await pgClient.query(ecocodeSql, [taxonId]);

        let ecocodes = {
            systems: [],
        };
        ecocodeRes.rows.forEach(ecocodeRow => {
            //if system doesn't already exist in the array, add it
            let systemExists = false;
            ecocodes.systems.forEach(system => {
                if(system.ecocode_system_id == ecocodeRow.ecocode_system_id) {
                    systemExists = true;
                }
            });
            if(!systemExists) {
                ecocodes.systems.push({
                    ecocode_system_id: ecocodeRow.ecocode_system_id,
                    name: ecocodeRow.system_name,
                    definition: ecocodeRow.system_definition,
                    notes: ecocodeRow.system_notes,
                    groups: []
                });
            }
        });

        //now add the groups to each system
        ecocodes.systems.forEach(system => {
            ecocodeRes.rows.forEach(ecocodeRow => {
                if(ecocodeRow.ecocode_system_id == system.ecocode_system_id) {
                    //if group doesn't already exist in the array, add it
                    let groupExists = false;
                    system.groups.forEach(group => {
                        if(group.ecocode_group_id == ecocodeRow.ecocode_group_id) {
                            groupExists = true;
                        }
                    });
                    if(!groupExists) {
                        system.groups.push({
                            ecocode_group_id: ecocodeRow.ecocode_group_id,
                            name: ecocodeRow.ecocode_group_name,
                            definition: ecocodeRow.ecocode_group_definition,
                            notes: ecocodeRow.ecocode_group_notes,
                            codes: []
                        });
                    }
                }
            });
        });

        //now add the codes to each group
        ecocodes.systems.forEach(system => {
            system.groups.forEach(group => {
                ecocodeRes.rows.forEach(ecocodeRow => {
                    if(ecocodeRow.ecocode_group_id == group.ecocode_group_id) {
                        //if code doesn't already exist in the array, add it
                        let codeExists = false;
                        group.codes.forEach(code => {
                            if(code.ecocode_id == ecocodeRow.ecocode_id) {
                                codeExists = true;
                            }
                        });
                        if(!codeExists && ecocodeRow.ecocode_system_id == system.ecocode_system_id) {
                            group.codes.push({
                                ecocode_id: ecocodeRow.ecocode_id,
                                ecocode_definition_id: ecocodeRow.ecocode_definition_id,
                                name: ecocodeRow.ecocode_name,
                                abbreviation: ecocodeRow.ecocode_abbreviation,
                                definition: ecocodeRow.ecocode_definition,
                                notes: ecocodeRow.ecocode_notes
                            });
                        }
                    }
                });
            });
        });

        taxon.ecocodes = ecocodes;

        //fetch rdb status
        let rdbSql = `SELECT 
        tbl_rdb.*,
        tbl_rdb_codes.rdb_category,
        tbl_rdb_codes.rdb_definition,
        tbl_rdb_codes.rdb_system_id,
        tbl_rdb_systems.rdb_system_id,
        tbl_rdb_systems.rdb_system,
        tbl_rdb_systems.biblio_id AS rdb_system_biblio_id,
        tbl_locations.location_name,
        tbl_locations.location_type_id,
        tbl_location_types.location_type,
        tbl_location_types.description AS location_type_description,
        tbl_biblio.bugs_reference,
        tbl_biblio.title AS biblio_title,
        tbl_biblio.year AS biblio_year,
        tbl_biblio.authors AS biblio_authors,
        tbl_biblio.full_reference AS biblio_full_reference,
        tbl_biblio.notes AS biblio_notes
        FROM tbl_rdb
        JOIN tbl_rdb_codes ON tbl_rdb_codes.rdb_code_id=tbl_rdb.rdb_code_id
        JOIN tbl_rdb_systems ON tbl_rdb_systems.rdb_system_id=tbl_rdb_codes.rdb_system_id
        JOIN tbl_locations ON tbl_locations.location_id=tbl_rdb.location_id
        JOIN tbl_location_types ON tbl_location_types.location_type_id=tbl_locations.location_type_id
        JOIN tbl_biblio ON tbl_biblio.biblio_id=tbl_rdb_systems.biblio_id
        WHERE taxon_id=$1
        `;
        let speciesRdb = await pgClient.query(rdbSql, [taxonId]);
        taxon.rdb = speciesRdb.rows;

        //tbl_taxa_images
        taxon.images = await this.fetchFromTable("tbl_taxa_images", "taxon_id", taxonId);


        taxon.measured_attributes = await this.fetchFromTable("tbl_taxa_measured_attributes", "taxon_id", taxonId);

        let res = await pgClient.query('SELECT tbl_taxa_common_names.*, tbl_languages.language_name_english FROM tbl_taxa_common_names left join tbl_languages on tbl_taxa_common_names.language_id=tbl_languages.language_id where taxon_id=$1', [taxonId]);
        taxon.common_names = res.rows;

        if(parseInt(taxon.author_id)) {
            taxon.taxa_tree_authors = await this.fetchFromTable("tbl_taxa_tree_authors", "author_id", taxon.author_id);
        }

        //tbl_taxa_seasonality
        taxon.taxa_seasonality = await this.fetchFromTable("tbl_taxa_seasonality", "taxon_id", taxonId);

        //tbl_taxa_reference_specimens
        taxon.taxa_reference_specimens = await this.fetchFromTable("tbl_taxa_reference_specimens", "taxon_id", taxonId);

        //tbl_activity_types
        for(let key in taxon.taxa_seasonality) {
             let activityTypeRes = await this.fetchFromTable("tbl_activity_types", "activity_type_id", taxon.taxa_seasonality[key].activity_type_id);
             taxon.taxa_seasonality[key].activity_type = activityTypeRes[0];
        }


        //tbl_seasons && tbl_season_types
        for(let key in taxon.taxa_seasonality) {
            const sql = `
                SELECT 
                tbl_seasons.*,
                tbl_season_types.season_type,
                tbl_season_types.description
                FROM tbl_seasons 
                LEFT JOIN tbl_season_types ON tbl_season_types.season_type_id = tbl_seasons.season_type_id
                WHERE tbl_seasons.season_id=$1
                `;
            let res = await pgClient.query(sql, [taxon.taxa_seasonality[key].season_id]);
            taxon.taxa_seasonality[key].season = res.rows[0];
        }

        for(let key in taxon.taxa_seasonality) {
            const sql = `SELECT * FROM tbl_locations WHERE location_id=$1`;
            let res = await pgClient.query(sql, [taxon.taxa_seasonality[key].location_id]);
            taxon.taxa_seasonality[key].location = res.rows[0];
        }

        //tbl_text_distribution
        taxon.distribution = await this.fetchFromTable("tbl_text_distribution", "taxon_id", taxonId);
        //Fetch biblio for each distribution item
        for(let key in taxon.distribution) {
            let biblioRes = await this.fetchFromTable("tbl_biblio", "biblio_id", taxon.distribution[key].biblio_id);
            taxon.distribution[key].biblio = biblioRes[0];
        }

        //tbl_text_biology
        taxon.biology = await this.fetchFromTable("tbl_text_biology", "taxon_id", taxonId);
        //Fetch biblio for each biology item
        for(let key in taxon.biology) {
            let biblioRes = await this.fetchFromTable("tbl_biblio", "biblio_id", taxon.biology[key].biblio_id);
            taxon.biology[key].biblio = biblioRes[0];
        }

        //tbl_taxonomy_notes
        taxon.taxonomy_notes = await this.fetchFromTable("tbl_taxonomy_notes", "taxon_id", taxonId);
        //Fetch biblio for each taxonomy_notes item
        for(let key in taxon.taxonomy_notes) {
            let biblioRes = await this.fetchFromTable("tbl_biblio", "biblio_id", taxon.taxonomy_notes[key].biblio_id);
            taxon.taxonomy_notes[key].biblio = biblioRes[0];
        }

        //tbl_taxonomic_order
        taxon.taxonomic_order = await this.fetchFromTable("tbl_taxonomic_order", "taxon_id", taxonId);

        //tbl_taxonomic_order_systems
        for(let key in taxon.taxonomic_order) {
            taxon.taxonomic_order[key].system = await this.fetchFromTable("tbl_taxonomic_order_systems", "taxonomic_order_system_id", taxon.taxonomic_order[key].taxonomic_order_system_id);
        }
        
        //tbl_taxonomic_order_biblio
        //taxon.taxonomic_order = await this.fetchFromTable("tbl_taxonomic_order_biblio", "taxon_id", taxonId);

        //tbl_text_identification_keys
        taxon.text_identification_keys = await this.fetchFromTable("tbl_text_identification_keys", "taxon_id", taxonId);


        //tbl_species_associations
        let sql = `SELECT 
        tbl_species_associations.*,
        tbl_species_association_types.association_description,
        tbl_species_association_types.association_type_name,
		tbl_taxa_tree_master.genus_id,
		tbl_taxa_tree_master.species,
		tbl_taxa_tree_genera.family_id,
		tbl_taxa_tree_genera.genus_name,
		tbl_taxa_tree_families.order_id,
		tbl_taxa_tree_families.family_name,
		tbl_taxa_tree_orders.order_name,
        tbl_taxa_tree_orders.record_type_id
        FROM tbl_species_associations
        JOIN tbl_species_association_types ON tbl_species_association_types.association_type_id = tbl_species_associations.association_type_id
        JOIN tbl_taxa_tree_master ON tbl_taxa_tree_master.taxon_id=tbl_species_associations.taxon_id
		JOIN tbl_taxa_tree_genera ON tbl_taxa_tree_genera.genus_id=tbl_taxa_tree_master.genus_id
		JOIN tbl_taxa_tree_families ON tbl_taxa_tree_families.family_id=tbl_taxa_tree_genera.family_id
		JOIN tbl_taxa_tree_orders ON tbl_taxa_tree_orders.order_id=tbl_taxa_tree_families.order_id
        WHERE tbl_species_associations.taxon_id=$1
        `;
        let speciesAssociationsRes = await pgClient.query(sql, [taxonId]);

        //Return associated taxa in the same format as in the site reports
        taxon.species_associations = [];
        for(let key in speciesAssociationsRes.rows) {
            let associatedTaxon = speciesAssociationsRes.rows[key];
            let associatedTaxonObject = {
                "association_type_id": associatedTaxon.association_type_id,
                "association_type_name": associatedTaxon.association_type_name,
                "association_description": associatedTaxon.association_description,
                "referencing_type": associatedTaxon.referencing_type,
                "biblio_id": associatedTaxon.biblio_id,

                "taxon_id": associatedTaxon.taxon_id,
                "genus_id": associatedTaxon.genus_id,
                "species": associatedTaxon.species,
                "genus": {
                    "genus_id": associatedTaxon.genus_id,
                    "genus_name": associatedTaxon.genus_name
                },
                "family": {
                    "family_id": associatedTaxon.family_id,
                    "family_name": associatedTaxon.family_name
                },
                "order": {
                    "order_id": associatedTaxon.order_id,
                    "order_name": associatedTaxon.order_name,
                    "record_type_id": associatedTaxon.record_type_id
                }
            }
            taxon.species_associations.push(associatedTaxonObject);
        }

        //tbl_biblio

        this.releaseDbConnection(pgClient);

        if(this.useTaxaCaching) {
            //Store in cache
            this.saveObjectToCache("taxa", identifierObject, taxon);
        }

        return taxon;
    }

    async fetchFromTable(table, column, value) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }
        const sql = 'SELECT * FROM '+table+' where '+column+'=$1';
        let res = await pgClient.query(sql, [value]);
        this.releaseDbConnection(pgClient);
        return res.rows;
    }

    async searchTaxa(attribute, value) {
        console.log("Taxa search for attribute", attribute, "with value", value);
        let taxaIds = [];
        let query = {}
        
        let findMode = "$eq";
        if(value.toString().substring(0, 4) == "not:") {
            findMode = "$ne";
            value = value.substring(4);
        }
        if(value.toString().substring(0, 3) == "gt:") {
            findMode = "$gt";
            value = value.substring(3);
        }
        if(value.toString().substring(0, 3) == "lt:") {
            findMode = "$lt";
            value = value.substring(3);
        }

        if(!isNaN(parseInt(value))) {
            value = parseInt(value);
        }
        else if(!isNaN(parseFloat(value))) {
            value = parseFloat(value);
        }
        else {
            value = "\""+value.toString()+"\"";
        }

        query = "{ \""+attribute+"\": { \""+findMode+"\": "+value+" } }";
        query = JSON.parse(query);
        let res = await this.mongo.collection('taxa').find(query);
        let taxa = await res.toArray();
        taxa.forEach(taxon => {
            taxaIds.push({
                taxon_id: taxon.taxon_id,
                link: "https://api.supersead.humlab.umu.se/taxon/"+taxon.taxon_id
            });
        });
        return taxaIds;
    }

    async searchSites(search, value) {
        console.log("Searching in", search, "for the value", value);
        let siteIds = [];
        let query = {}
        
        let findMode = "$eq";
        if(value.toString().substring(0, 4) == "not:") {
            findMode = "$ne";
            value = value.substring(4);
        }
        if(value.toString().substring(0, 3) == "gt:") {
            findMode = "$gt";
            value = value.substring(3);
        }
        if(value.toString().substring(0, 3) == "lt:") {
            findMode = "$lt";
            value = value.substring(3);
        }

        if(!isNaN(parseInt(value))) {
            value = parseInt(value);
        }
        else if(!isNaN(parseFloat(value))) {
            value = parseFloat(value);
        }
        else {
            value = "\""+value.toString()+"\"";
        }

        query = "{ \""+search+"\": { \""+findMode+"\": "+value+" } }";
        query = JSON.parse(query);
        let res = await this.mongo.collection('sites').find(query);
        let sites = await res.toArray();
        sites.forEach(site => {
            siteIds.push({
                site_id: site.site_id,
                link: "https://api.supersead.humlab.umu.se/site/"+site.site_id
            });
        });
        return siteIds;
    }

    
    async flushTaxaCache() {
        console.log("Flushing taxa cache");
        if(this.cacheStorageMethod == "mongo") {
            await this.mongo.collection('taxa').deleteMany({});
        }
        if(this.cacheStorageMethod == "file") {
            let files = fs.readdirSync("taxa_cache");
            for(let key in files) {
                fs.unlinkSync("taxa_cache/"+files[key]);
            }
        }
    }

    async flushSiteCache() {
        console.log("Flushing site cache");
        if(this.cacheStorageMethod == "mongo") {
            await this.mongo.collection('sites').deleteMany({});
        }
        if(this.cacheStorageMethod == "file") {
            let files = fs.readdirSync("site_cache");
            for(let key in files) {
                fs.unlinkSync("site_cache/"+files[key]);
            }
        }
    }

    async preloadAllTaxa() {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        console.time("Preload of taxa complete");
        console.log("Preloading taxa");

        let taxaData = await pgClient.query('SELECT * FROM tbl_taxa_tree_master ORDER BY taxon_id');
        this.releaseDbConnection(pgClient);

        console.log("Will fetch taxa data for "+taxaData.rows.length+" sites");

        let taxonIds = [];
        for(let key in taxaData.rows) {
            taxonIds.push(taxaData.rows[key].taxon_id);
        }

        let pendingFetches = 0;

        await new Promise((resolve, reject) => {
            const fetchCheckInterval = setInterval(() => {
                if(taxonIds.length > 0 && pendingFetches < this.maxConcurrentFetches) {
                    pendingFetches++;
                    let taxonId = taxonIds.shift();
                    console.time("Fetched taxon "+taxonId);
                    this.getTaxon(taxonId, false).then(() => {
                        console.timeEnd("Fetched taxon "+taxonId);
                        pendingFetches--;
                    }).catch(error => {
                        console.log("Failed fetching taxon "+taxonId);
                        console.log(error);
                    });
                }
                if(taxonIds.length == 0) {
                    clearInterval(fetchCheckInterval);
                    resolve();
                }
            }, 10);
        });

        console.timeEnd("Preload of taxa complete");
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

        let pendingFetches = 0;

        await new Promise((resolve, reject) => {
            const fetchCheckInterval = setInterval(() => {
                if(siteIds.length > 0 && pendingFetches < this.maxConcurrentFetches) {
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
            }, 10);
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
            console.error("Failed to get Postgres DB connection!");
            return false;
        }
        if(verbose) console.time("Fetched basic site data for site "+siteId);
        let siteData = await pgClient.query('SELECT * FROM tbl_sites WHERE site_id=$1', [siteId]);
        site = siteData.rows[0];
        site.data_groups = [];
        site.api_source = appName+"-"+appVersion;
        site.lookup_tables = {
            biblio: []
        };

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
        
        if(verbose) console.time("Fetched analysis entities prep methods for site "+siteId);
        await this.fetchAnalysisEntitiesPrepMethods(site);
        if(verbose) console.timeEnd("Fetched analysis entities prep methods for site "+siteId);

        if(verbose) console.time("Fetched other records for site "+siteId);
        await this.fetchSiteOtherRecords(site);
        if(verbose) console.timeEnd("Fetched other records for site "+siteId);

        /*
        if(verbose) console.time("Fetched analysis entities ages for site "+siteId);
        await this.fetchAnalysisEntitiesAges(site);
        if(verbose) console.timeEnd("Fetched analysis entities ages for site "+siteId);
        */

        if(verbose) console.time("Fetched analysis methods for site "+siteId);
        await this.fetchAnalysisMethods(site);
        if(verbose) console.timeEnd("Fetched analysis methods for site "+siteId);

        //tbl_site_other_records

        if(fetchMethodSpecificData) {
            if(verbose) console.time("Fetched method specific data for site "+siteId);
            await this.fetchMethodSpecificData(site);
            if(verbose) console.timeEnd("Fetched method specific data for site "+siteId);
        }

        if(verbose) console.time("Done post-processing primary data for site "+siteId);
        this.postProcessSiteData(site);
        if(verbose) console.timeEnd("Done post-processing primary data for site "+siteId);

        if(verbose) console.timeEnd("Done fetching site "+siteId);

        if(this.useSiteCaching) {
            //Store in cache
            this.saveSiteToCache(site);
        }

        return site;
    }

    async fetchMethodByMethodId(method_id) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }
        let sql = `SELECT tbl_methods.*,
        tbl_units.description AS unit_desc,
        tbl_units.unit_id,
        tbl_units.unit_abbrev,
        tbl_units.unit_name
        FROM tbl_methods
        LEFT JOIN tbl_units ON tbl_units.unit_id=tbl_methods.unit_id
        WHERE method_id=$1
        `;
        let res = await pgClient.query(sql, [method_id]);

        if(res.rows.length == 0) {
            console.warn("No method found for method_id", method_id);
            return false;
        }

        let method = {
            method_id: res.rows[0].method_id,
            biblio_id: res.rows[0].biblio_id,
            method_name: res.rows[0].method_name,
            description: res.rows[0].description,
            method_abbrev_or_alt_name: res.rows[0].method_abbrev_or_alt_name,
            method_group_id: res.rows[0].method_group_id,
            record_type_id: res.rows[0].record_type_id,
            unit_id: res.rows[0].unit_id,
        };

        this.releaseDbConnection(pgClient);
        return method;
    }

    getCoordinateMethodByMethodId(site, method_id) {
        for(let key in site.lookup_tables.coordinate_methods) {
            if(site.lookup_tables.coordinate_methods[key].method_id == method_id) {
                return site.lookup_tables.coordinate_methods[key];
            }
        }
        return false;
    }

    getAnalysisMethodByMethodId(site, method_id) {
        for(let key in site.lookup_tables.analysis_methods) {
            if(site.lookup_tables.analysis_methods[key].method_id == method_id) {
                return site.lookup_tables.analysis_methods[key];
            }
        }
        return false;
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
            sampleGroup.datasets = []; //Will use this further down
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

        //Link sample_groups to analyses via their physical_samples_ids and analysis_entities
        //this is just to make it more convenient/performant for the client since this requires a lot of looping to figure out
        site.sample_groups.forEach(sampleGroup => {
            //For each sample_group, check which datasets it can be linked to
            datasets.forEach(dataset => {
                dataset.analysis_entities.forEach(datasetAe => {
                    sampleGroup.physical_samples.forEach(physicalSample => {
                        physicalSample.analysis_entities.forEach(sgAe => {
                            if(datasetAe.analysis_entity_id == sgAe.analysis_entity_id) {
                                if(!sampleGroup.datasets.includes(dataset.dataset_id)) {
                                    sampleGroup.datasets.push(dataset.dataset_id);
                                }
                            }
                        });
                    });
                });
            });
        });

        //Unlink analysis_entities from samples
        site.sample_groups.forEach(sg => {
            sg.physical_samples.forEach(ps => {
                delete ps.analysis_entities;
            })
        })

        //Here we give each module a chance to modify the data structure now that everything is fetched
        for(let key in this.dataFetchingModules) {
            let module = this.dataFetchingModules[key];
            module.postProcessSiteData(site);
        }
        return site;
    }

    async fetchSiteOtherRecords(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }
        let sql = `
        SELECT 
        tbl_site_other_records.biblio_id,
        tbl_site_other_records.record_type_id,
        tbl_site_other_records.description,
        tbl_record_types.record_type_name,
        tbl_record_types.record_type_description
        FROM tbl_site_other_records
        INNER JOIN tbl_record_types ON tbl_record_types.record_type_id = tbl_site_other_records.record_type_id
        WHERE site_id=$1
        `;

        let data = await pgClient.query(sql, [site.site_id]);
        site.other_records = data.rows;

        site.other_records.forEach(otherRec => {
            if(parseInt(otherRec.biblio_id)) {
                pgClient.query("SELECT * FROM tbl_biblio WHERE biblio_id=$1", [otherRec.biblio_id]).then(biblioData => {
                    otherRec.biblio = biblioData.rows[0];
                });
            }
        })

        this.releaseDbConnection(pgClient);
        
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

        if(typeof site.lookup_tables.biblio == "undefined") {
            site.lookup_tables.biblio = [];
        }

        site.biblio.forEach(biblio => {
            let found = false;
            site.lookup_tables.biblio.forEach(existingBiblio => {
                if(existingBiblio.biblio_id == biblio.biblio_id) {
                    found = true;
                }
            })
            if(!found) {
                this.fetchBiblio(biblio.biblio_id).then(b => {
                    site.lookup_tables.biblio.push(b);
                });
            }
        });

        this.releaseDbConnection(pgClient);
        
        return site;
    }

    async fetchBiblio(biblioId) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }
        let sql = `
        SELECT * FROM tbl_biblio WHERE biblio_id=$1
        `;
        let biblio = await pgClient.query(sql, [biblioId]);
        this.releaseDbConnection(pgClient);
        return biblio.rows[0];
    }

    async fetchSampleGroups(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sampleGroups = await pgClient.query('SELECT * FROM tbl_sample_groups WHERE site_id=$1', [site.site_id]);
        site.sample_groups = sampleGroups.rows;

        let sampleGroupCoordinatesSql = `
        SELECT *,
        tbl_dimensions.method_group_id AS dimension_method_group_id,
        tbl_coordinate_method_dimensions.method_id AS coordinate_method_id
        FROM tbl_sample_group_coordinates
        JOIN tbl_coordinate_method_dimensions ON tbl_sample_group_coordinates.coordinate_method_dimension_id=tbl_coordinate_method_dimensions.coordinate_method_dimension_id
        JOIN tbl_methods ON tbl_methods.method_id=tbl_coordinate_method_dimensions.method_id
        JOIN tbl_dimensions ON tbl_dimensions.dimension_id=tbl_coordinate_method_dimensions.dimension_id
        JOIN tbl_units ON tbl_units.unit_id=tbl_dimensions.unit_id
        WHERE sample_group_id=$1`;

        for(let key in site.sample_groups) {
            let sampleGroupsCoords = await pgClient.query(sampleGroupCoordinatesSql, [site.sample_groups[key].sample_group_id]);

            site.sample_groups[key].coordinates = [];
            sampleGroupsCoords.rows.forEach(sgCoord => {
                site.sample_groups[key].coordinates.push({
                    accuracy: sgCoord.position_accuracy,
                    coordinate_method_id: sgCoord.coordinate_method_id,
                    dimension_id: sgCoord.dimension_id,
                    measurement: parseFloat(sgCoord.sample_group_position),
                });

                let coordMethod = this.getCoordinateMethodByMethodId(site, sgCoord.coordinate_method_id);
                if(!coordMethod) {
                    this.fetchMethodByMethodId(sgCoord.coordinate_method_id).then(method => {
                        if(method) {
                            this.addCoordinateMethodToLocalLookup(site, method);
                            if(parseInt(method.unit_id)) {
                                this.fetchUnit(method.unit_id).then(unit => {
                                    this.addUnitToLocalLookup(site, unit);
                                });
                                
                            }
                        }
                    })
                }

                let dimension = this.getDimensionByDimensionId(site, sgCoord.dimension_id);
                if(!dimension) {
                    this.fetchDimension(sgCoord.dimension_id).then(dimension => {
                        if(dimension) {
                            this.addDimensionToLocalLookup(site, dimension);
                        }
                    });
                }

                let unit = this.getUnitByUnitId(site, sgCoord.unit_id);
                if(!unit) {
                    this.fetchUnit(sgCoord.unit_id).then(unit => {
                        if(unit) {
                            this.addUnitToLocalLookup(site, unit);
                        }
                    });
                }

            });
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

            if(typeof site.lookup_tables.biblio == "undefined") {
                site.lookup_tables.biblio = [];
            }
            sampleGroupsRefs.rows.forEach(biblio => {
                let found = false;
                site.lookup_tables.biblio.forEach(existingBiblio => {
                    if(existingBiblio.biblio_id == biblio.biblio_id) {
                        found = true;
                    }
                })
                if(!found) {
                    site.lookup_tables.biblio.push(biblio);
                }
            });
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
                site.sample_groups[key].descriptions[key2].type_id = sampleGroupsDescType.rows[0].sample_group_description_type_id;
                site.sample_groups[key].descriptions[key2].type_name = sampleGroupsDescType.rows[0].type_name;
                site.sample_groups[key].descriptions[key2].type_description = sampleGroupsDescType.rows[0].type_description;
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
        
        for(let key in this.dataFetchingModules) {
            let module = this.dataFetchingModules[key];
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
        for(let key in methodIds) {
            let methodId = methodIds[key];
            let methodResult = await pgClient.query('SELECT * FROM tbl_methods WHERE method_id=$1', [methodId]);
            let method = methodResult.rows[0];

            //The below unit_id's which is hardcoded based on method_id is temporary - SHOULD be included in the db instead, so when it is, just remove this series of if-statements
            if(methodId == 109) {
                //pH (H2O) - there's no unit for ph in the db atm
            }
            if(methodId == 107) {
                //µS (H2O) - something about conductivity, these scientists man, they burn it, they pour acid over it, and they electrocute it... I'm surprised they're not trying to smash it with a rock as well
                //there doesn't seem to be any units for that fits, in the current sead db
            }
            if(methodId == 74) {
                //Total phosphates citric acid - assume it's ppm/micrograms
                method.unit_id = 18;
            }
            if(methodId == 37) {
                //Phosphate degrees - db says: "The amount of phosphate is specified as mg P2O5/100g dry soil.""
                method.unit_id = 17; //let's say it's just mg for now, which is true enough
            }
            if(methodId == 33) {
                //MS - assume the unit is milligrams
                method.unit_id = 17;
            }
            if(methodId == 32) {
                //LOI - assume the unit is a percentage
                method.unit_id = 23;
            }

            if(parseInt(method.unit_id)) {
                let methodUnitRes = await pgClient.query('SELECT * FROM tbl_units WHERE unit_id=$1', [method.unit_id]);
                let unit = methodUnitRes.rows[0];
                method.unit = unit;
            }

            methods.push(method);
        }
        /*
        methodIds.forEach(methodId => {
            let promise = pgClient.query('SELECT * FROM tbl_methods WHERE method_id=$1', [methodId]).then(method => {
                methods.push(method.rows[0]);
            });
            queryPromises.push(promise);
        });
        */

        await Promise.all(queryPromises).then(() => {
            this.releaseDbConnection(pgClient);
        });
        
        site.lookup_tables.analysis_methods = methods;

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

        let datasets = [];
        let sql = `
        SELECT
        tbl_datasets.*,
        tbl_methods.method_group_id
        FROM tbl_datasets 
        LEFT JOIN tbl_methods ON tbl_datasets.method_id = tbl_methods.method_id
        WHERE dataset_id=$1
        `;

        for(let key in datasetIds) {
            let datasetId = datasetIds[key];
            let datasetRes = await pgClient.query(sql, [datasetId]);
            if(datasetRes.rows.length > 0) {
                let dataset = datasetRes.rows[0];

                dataset.contacts = [];
                let datasetContactsRes = await pgClient.query("SELECT contact_id FROM tbl_dataset_contacts WHERE dataset_id=$1", [dataset.dataset_id]);
                datasetContactsRes.rows.forEach(r => {
                    dataset.contacts.push(r.contact_id);
                });

                datasets.push(dataset);
                if(dataset.biblio_id) {
                    let dsBib = await pgClient.query("SELECT * FROM tbl_biblio WHERE biblio_id=$1", [dataset.biblio_id]);
                    
                    dsBib.rows.forEach(biblio => {
                        let biblioFound = false;
                        for(let bKey in site.lookup_tables.biblio) {
                            if(site.lookup_tables.biblio[bKey].biblio_id == biblio.biblio_id) {
                                biblioFound = true;
                            }
                        }
                        if(!biblioFound) {
                            site.lookup_tables.biblio.push(biblio);
                        }
                    });
                }
            }
        }
        
        site.datasets = datasets;

        //Fetch dataset contacts and associated information, note that dataset contacts are different from dataset references!
        sql = `
        SELECT DISTINCT ON (tbl_dataset_contacts.contact_id) tbl_dataset_contacts.*,
        tbl_contact_types.contact_type_name AS contact_type,
        tbl_contact_types.description AS contact_type_description,
        tbl_contacts.address_1 AS contact_address_1,
        tbl_contacts.address_2 AS contact_address_2,
        tbl_contacts.location_id AS contact_location_id,
        tbl_contacts.email AS contact_email,
        tbl_contacts.first_name AS contact_first_name,
        tbl_contacts.last_name AS contact_last_name,
        tbl_contacts.phone_number AS contact_phone,
        tbl_contacts.url AS contact_url,
        tbl_locations.location_name AS contact_location_name
        FROM tbl_sites
        JOIN tbl_sample_groups ON tbl_sample_groups.site_id = tbl_sites.site_id
        JOIN tbl_physical_samples ON tbl_physical_samples.sample_group_id=tbl_sample_groups.sample_group_id
        JOIN tbl_analysis_entities ON tbl_analysis_entities.physical_sample_id=tbl_physical_samples.physical_sample_id
        JOIN tbl_datasets ON tbl_datasets.dataset_id=tbl_analysis_entities.dataset_id
        JOIN tbl_dataset_contacts ON tbl_dataset_contacts.dataset_id=tbl_datasets.dataset_id
		JOIN tbl_contact_types ON tbl_contact_types.contact_type_id=tbl_dataset_contacts.contact_type_id
		JOIN tbl_contacts ON tbl_contacts.contact_id=tbl_dataset_contacts.contact_id
        LEFT JOIN tbl_locations ON tbl_locations.location_id=tbl_contacts.location_id
        WHERE tbl_sites.site_id=$1
        `;

        let datasetContacts = await pgClient.query(sql, [site.site_id]);
        site.lookup_tables.dataset_contacts = datasetContacts.rows;

        this.releaseDbConnection(pgClient);

        return site;
    }

    async fetchSampleGroupsSamplingMethods(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let methods = [];
        let queryPromises = [];
        site.sample_groups.forEach(sampleGroup => {
            
            let promise = pgClient.query('SELECT * FROM tbl_methods WHERE method_id=$1', [sampleGroup.method_id]).then(method => {
                sampleGroup.sampling_method_id = method.rows[0].method_id;
                let found = false;
                for(let key in methods) {
                    if(methods[key].method_id == method.rows[0].method_id) {
                        found = true;
                    }
                }
                if(!found) {
                    methods.push(method.rows[0]);
                }
                
            });
            queryPromises.push(promise);
        });

        await Promise.all(queryPromises).then(() => {
            this.releaseDbConnection(pgClient);
        });

        site.lookup_tables.sampling_methods = methods;

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

    async fetchAnalysisEntitiesAges(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = `SELECT * FROM tbl_analysis_entity_ages WHERE analysis_entity_id=$1`;
        let oldestAge = null;
        let youngestAge = null;
        for(let sgKey in site.sample_groups) {
            let sampleGroup = site.sample_groups[sgKey];
            for(let psKey in sampleGroup.physical_samples) {
                let sample = sampleGroup.physical_samples[psKey];
                for(let aeKey in sample.analysis_entities) {
                    let analysisEntity = sample.analysis_entities[aeKey];
                    let result = await pgClient.query(sql, [analysisEntity.analysis_entity_id]);
                    analysisEntity.ages = [];
                    result.rows.forEach(age => {
                        let older = parseInt(age.age_older);
                        let younger = parseInt(age.age_younger);
                        if(older || younger) {
                            //dates are all (I assume) BCE
                            if(older && older > oldestAge) {
                                oldestAge = older;
                            }
                            if(younger && younger < youngestAge) {
                                youngestAge = younger;
                            }
                            analysisEntity.ages.push({
                                older: age.age_older,
                                younger: age.age_younger,
                                dating_specifier: age.dating_specifier
                            });
                        }
                    });
                }
            }
        }

        //Create an age summary for each site that is an aggregation of the AE ages
        site.age_summary = {
            older: oldestAge,
            younger: youngestAge
        }

        this.releaseDbConnection(pgClient);
    }

    async fetchAnalysisEntitiesPrepMethods(site) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = `SELECT * FROM tbl_analysis_entity_prep_methods WHERE analysis_entity_id=$1`;
        let prepMethodIds = [];
        for(let sgKey in site.sample_groups) {
            let sampleGroup = site.sample_groups[sgKey];
            for(let psKey in sampleGroup.physical_samples) {
                let sample = sampleGroup.physical_samples[psKey];
                for(let aeKey in sample.analysis_entities) {
                    let analysisEntity = sample.analysis_entities[aeKey];
                    let result = await pgClient.query(sql, [analysisEntity.analysis_entity_id]);
                    analysisEntity.prepMethods = [];
                    result.rows.forEach(method => {
                        if(analysisEntity.prepMethods.indexOf(method.method_id) === -1) {
                            analysisEntity.prepMethods.push(method.method_id);
                        }
                        if(prepMethodIds.indexOf(method.method_id) === -1) {
                            prepMethodIds.push(method.method_id);
                        }
                    });
                }
            }
        }

        //Also feth metadata about each prep method and store in lookup table
        if(typeof site.lookup_tables.prep_methods == "undefined") {
            site.lookup_tables.prep_methods = [];
        }
        for(let key in prepMethodIds) {
            let methodId = prepMethodIds[key];
            let sql = `SELECT * FROM tbl_methods WHERE method_id=$1`;
            let result = await pgClient.query(sql, [methodId]);
            site.lookup_tables.prep_methods.push(result.rows[0]);
        }

        this.releaseDbConnection(pgClient);
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
                
                //If we have dimensions, then we need the units for these dimensions
                for(let key in sample.dimensions) {
                    let unit = this.getUnitByUnitId(site, sample.dimensions[key].unit_id);
                    if(!unit) {
                        this.fetchUnit(sample.dimensions[key].unit_id).then(unit => {
                            if(unit) {
                                this.addUnitToLocalLookup(site, unit);
                            }
                        });
                    }
                }

                //fetch sample coordinates for those which have local (or global) coordinate positioning
                sql = `SELECT
                tbl_sample_coordinates.measurement::float,
                tbl_sample_coordinates.accuracy,
                tbl_coordinate_method_dimensions.dimension_id,
                tbl_coordinate_method_dimensions.method_id AS coordinate_method_id
                FROM tbl_sample_coordinates
                JOIN tbl_coordinate_method_dimensions ON tbl_sample_coordinates.coordinate_method_dimension_id = tbl_coordinate_method_dimensions.coordinate_method_dimension_id
                WHERE tbl_sample_coordinates.physical_sample_id=$1`;

                let sampleCoordinates = await pgClient.query(sql, [sample.physical_sample_id]);
                sample.coordinates = sampleCoordinates.rows;

                for(let key in sample.coordinates) {
                    //find dimensions in lookup tables, and if not found, fetch them
                    let dimension = this.getDimensionByDimensionId(site, sample.coordinates[key].dimension_id);
                    if(!dimension) {
                        dimension = await this.fetchDimension(sample.coordinates[key].dimension_id);
                        site.lookup_tables.dimensions.push(dimension);
                    }
                }

                if(typeof site.lookup_tables.coordinate_methods == "undefined") {
                    site.lookup_tables.coordinate_methods = [];
                }
                for(let key in sample.coordinates) {
                    let method = this.getCoordinateMethodByMethodId(site, sample.coordinates[key].coordinate_method_id);
                    if(!method) {
                        method = await this.fetchMethodByMethodId(sample.coordinates[key].coordinate_method_id);
                        site.lookup_tables.coordinate_methods.push(method);
                    }
                }
            }
        }
        
        this.releaseDbConnection(pgClient);
        return site;
    }


    async fetchDimension(dimension_id) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let dimension = null;

        let sql = `SELECT *
        FROM tbl_dimensions
        WHERE dimension_id=$1
        `;
        let res = await pgClient.query(sql, [dimension_id]);
        this.releaseDbConnection(pgClient);
        if(res.rows.length > 0) {
            dimension = res.rows[0];
        }
        else {
            console.warn("Dimension with id "+dimension_id+" not found in db");
        }
        
        return dimension;
    }

    getDimensionByDimensionId(site, dimension_id) {
        if(typeof site.lookup_tables.dimensions == "undefined") {
            site.lookup_tables.dimensions = [];
        }
        for(let key in site.lookup_tables.dimensions) {
            if(site.lookup_tables.dimensions[key].dimension_id == dimension_id) {
                return site.lookup_tables.dimensions[key];
            }
        }
        return null;
    }

    fetchMethod(site, method_id) {
        let method = null;
        site.lookup_tables.analysis_methods.forEach(m => {
            if(m.method_id == method_id) {
                method = m;
            }
        });
        return method;
    }

    async fetchUnit(unit_id) {
        let pgClient = await this.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let unit = null;

        let sql = `SELECT *
        FROM tbl_units
        WHERE unit_id=$1
        `;
        let unitRes = await pgClient.query(sql, [unit_id]);
        this.releaseDbConnection(pgClient);
        if(unitRes.rows.length > 0) {
            unit = unitRes.rows[0];
        }
        
        return unit;
    }

    getUnitByUnitId(site, unit_id) {
        if(typeof site.lookup_tables.units == "undefined") {
            site.lookup_tables.units = [];
        }
        for(let key in site.lookup_tables.units) {
            if(site.lookup_tables.units[key].unit_id == unit_id) {
                return site.lookup_tables.units[key];
            }
        }
        return null;
    }

    addCoordinateMethodToLocalLookup(site, method) {
        if(typeof site.lookup_tables.coordinate_methods == "undefined") {
            site.lookup_tables.coordinate_methods = [];
        }
        if(!this.getCoordinateMethodByMethodId(site, method.method_id)) {
            site.lookup_tables.coordinate_methods.push(method);
        }
    }

    addDimensionToLocalLookup(site, dimension) {
        if(typeof site.lookup_tables.dimensions == "undefined") {
            site.lookup_tables.dimensions = [];
        }
        if(!this.getDimensionByDimensionId(site, dimension.dimension_id)) {
            site.lookup_tables.dimensions.push(dimension);
        }
    }

    addUnitToLocalLookup(site, unit) {
        if(typeof site.lookup_tables.units == "undefined") {
            site.lookup_tables.units = [];
        }
        if(this.getUnitByUnitId(site, unit.unit_id) == null) {
            site.lookup_tables.units.push(unit);
        }
    }

    async getObjectFromCache(collection, identifierObject, findMany = false) {
        if(this.allCachingDisabled) {
            return false;
        }
        let foundObject = null;
        let findResult = await this.mongo.collection(collection).find(identifierObject).toArray();
        if(findResult.length > 0 && findMany == false) {
            foundObject = findResult[0];
        }
        else if(findMany) {
            return findResult;
        }

        if(foundObject && (this.checkVersionMatch(foundObject.api_source, appName+"-"+appVersion) || this.acceptCacheVersionMismatch)) {
            return foundObject;
        }
        return false;
    }

    checkVersionMatch(version1, version2) {
        //We allow a patch-level mismatch
        let parts = version1.split(".");
        let v1 = parts[0]+"."+parts[1];

        parts = version2.split(".");
        let v2 = parts[0]+"."+parts[1];

        return v1 == v2;
    }

    async saveObjectToCache(collection, identifierObject, saveObject) {
        if(this.allCachingDisabled) {
            return false;
        }
        saveObject.api_source = appName+"-"+appVersion;
        await this.mongo.collection(collection).deleteMany(identifierObject);
        this.mongo.collection(collection).insertOne(saveObject);
    }

    async getSiteFromCache(siteId) {
        return await this.getObjectFromCache("sites", {
            site_id: parseInt(siteId)
        });
    }

    async saveSiteToCache(site) {
        if(this.cacheStorageMethod == "mongo") {
            await this.mongo.collection('sites').deleteOne({ site_id: parseInt(site.site_id) });
            this.mongo.collection('sites').insertOne(site);
        }
        if(this.cacheStorageMethod == "file") {
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

    async getSitesWithSampleCoordinates() {
        let results = null;
        try {
            const pipeline = [
                { "$unwind": "$sample_groups" },
                { "$unwind": "$sample_groups.physical_samples" },
                { "$match": { "sample_groups.physical_samples.coordinates.0": { "$exists": true } } },
                { "$project": { "site_id": 1 } }
            ];
    
            results = await this.mongo.collection("sites").aggregate(pipeline).toArray();
            console.log(results);

        } catch (e) {
            console.error(e);
        }

        return results;
    }

    async flushGraphCache() {
        console.log("Flushing graph cache");
        if(this.cacheStorageMethod == "mongo") {
            await this.mongo.collection('graph_cache').deleteMany({});
        }
    }

    async setupDatabase() {
        console.log("Setup postgres database");
        this.pgConnections = 0;
        try {
            this.pgPool = new Pool({
                user: process.env.POSTGRES_USER,
                host: process.env.POSTGRES_HOST,
                database: process.env.POSTGRES_DATABASE,
                password:process.env.POSTGRES_PASS,
                port: process.env.POSTGRES_PORT,
                max: process.env.POSTGRES_MAX_CONNECTIONS,
                idleTimeoutMillis: 1000,
                connectionTimeoutMillis: 0, //no timeout
            });

            this.pgPool.on('connect', client => {
                //console.log("Pg connect");
            });
            /*
            this.pgPool.on('acquire', client => {
                this.pgConnections++;
                console.log("Pg acquire", this.pgConnections);
            });
            this.pgPool.on('remove', client => {
                this.pgConnections--;
                console.log("Pg remove", this.pgConnections);
            });
            */

            if(this.useStaticDbConnection) {
                console.log("Setting up static postgres db connection");
                this.staticDbConnection = await this.pgPool.connect();
                console.log("Static postgres database ready");
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
            //check health of postgres connection
            if(!this.staticDbConnection) {
                this.staticDbConnection = await this.pgPool.connect();

                //handle errors gracefully
                this.staticDbConnection.on('error', (err) => {
                    console.error("Static postgres connection error");
                    console.error(err);
                    this.staticDbConnection = null;
                    this.getDbConnection();
                });
            }

            let healthCheck = await this.staticDbConnection.query("SELECT 1");
            if(healthCheck.rows[0]['?column?'] != 1) {
                console.warn("Static postgres connection is not healthy, attempting to reconnect.");
                this.staticDbConnection = await this.pgPool.connect();
            }
            return this.staticDbConnection;
        }
        else {
            let dbcon = false;
            try {
                dbcon = await this.pgPool.connect();
                if(dbcon) {
                    this.pgConnections++;
                    //console.log(this.pgConnections)
                }
            }
            catch(error) {
                console.error("Couldn't connect to postgres database");
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
            this.pgConnections--;
            //console.log(this.pgConnections)
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
        console.log("Shutdown called");
        this.pgPool.end();
        this.server.close(() => {
            console.log('Server shutdown');
            process.exit(0);
        });
    }
}

module.exports = SeadJsonApiServer;