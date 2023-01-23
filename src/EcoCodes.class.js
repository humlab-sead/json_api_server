class EcoCodes {
    constructor(app) {
        this.app = app;

        this.app.expressApp.get('/ecocodes/taxon/:taxonid', async (req, res) => {
            let taxonId = parseInt(req.params.taxonid);
            if(taxonId) {
                let ecoCodes = await this.getEcoCodesForTaxon(taxonId);
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/ecocodes/site/:siteid', async (req, res) => {
            let siteId = parseInt(req.params.siteid);
            if(siteId) {
                let ecoCodes = await this.getEcoCodesForSite(siteId);
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/ecocodes/site/:siteid/samples', async (req, res) => {
            let siteId = parseInt(req.params.siteid);
            if(siteId) {
                let ecoCodes = await this.getEcoCodesForSiteGroupedBySample(siteId);
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(ecoCodes, null, 2));
            }
        });

        this.app.expressApp.get('/preload/ecocodes/:flushCache?', async (req, res) => {
            console.log(req.path);
            if(req.params.flushCache) {
                //await this.flushEcocodeCache(); //not implemented
            }
            await this.preloadAllSiteEcocodes();
            res.send("Preload of ecocodes complete");
        });
    }

    async preloadAllSiteEcocodes() {
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

        let maxConcurrentFetches = parseInt(process.env.MAX_CONCURRENT_FETCHES);
        maxConcurrentFetches = isNaN(maxConcurrentFetches) == false ? maxConcurrentFetches : 10;

        let pendingFetches = 0;

        await new Promise((resolve, reject) => {
            const fetchCheckInterval = setInterval(() => {
                if(siteIds.length > 0 && pendingFetches < maxConcurrentFetches) {
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

    async getEcoCodesForTaxon(taxonId) {
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

    async getEcoCodesForSites(siteIds) {

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
            this.getEcoCodesForTaxon(taxonAbundance.taxon_id).then(taxonEcoCodes => {
                
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

                            let promise = this.getEcoCodesForTaxon(abundance.taxon_id);
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
                    let promise = this.getEcoCodesForTaxon(abundance.taxon_id);
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
            ecocode_bundles: ecocodeBundles
        };

        if(this.app.useEcoCodeCaching) {
            this.app.saveObjectToCache("site_ecocode_bundles", { site_id: parseInt(siteId) }, bundleObject);
        }
        
        return bundleObject;
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