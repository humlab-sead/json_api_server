const Taxa = require('./Taxa.class.js');

class EcoCodes {
    constructor(app) {
        this.app = app;
        this.taxaModule = new Taxa(app);

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

        this.app.expressApp.post('/ecocodes/sites/:aggregationType', async (req, res) => {
            let siteIds = req.body;
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
            });

            let data = await this.fetchEcoCodesForSites(siteIds, req.params.aggregationType);
            res.header("Content-type", "application/json");
            res.end(JSON.stringify(data, null, 2));
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

    async fetchEcoCodesForSitesOLD(siteIds) {
        console.log("getEcoCodesForSites");

        let bundles = await this.app.getObjectFromCache("site_ecocode_bundles", { site_id : { $in : siteIds } }, true);

        let ecocodes = [];

        //timespan
        let timeBinsNum = 10;
        let timeBins = [];

        //Find out what the max/min (oldest/youngest) points are in this total timespan
        bundles.sort((a, b) => {
            if(a.site_age_summary.oldest > b.site_age_summary.oldest) {
                return 1;
            }
            if(a.site_age_summary.oldest < b.site_age_summary.oldest) {
                return -1;
            }
        });
        let oldest = bundles[0].site_age_summary.oldest;
        
        bundles.sort((a, b) => {
            if(a.site_age_summary.youngest < b.site_age_summary.youngest) {
                return 1;
            }
            if(a.site_age_summary.youngest > b.site_age_summary.youngest) {
                return -1;
            }
        });
        let youngest = bundles[0].site_age_summary.youngest;

        let binSize = (oldest - youngest) / timeBinsNum;

        for(let i = 0; i < timeBinsNum; i++) {
            timeBins.push({
                older: oldest - (binSize * i),
                younger: (oldest - (binSize * i)) - binSize,
                ecocodes: []
            });
        }

        bundles.forEach(bundle => {
            //bin these by time

            //bundle.ecocode_bundles
        });
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

module.exports = EcoCodes;