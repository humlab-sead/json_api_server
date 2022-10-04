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


    async getEcoCodesForSite(siteId, aggregationMode = "numberOfSpecies") {
        if(this.app.useEcoCodeCaching) {
            let ecoCodeSiteBundle = await this.getEcocodeBundlesFromCache(siteId);
            if(ecoCodeSiteBundle) {
                return ecoCodeSiteBundle;
            }
        }

        let site = await this.app.getSiteFromCache(siteId);
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

        let ecocodeBundles = [];
        let fetchPromises = [];
        datasets.forEach(dataset => {
            dataset.analysis_entities.forEach(ae => {
                ae.abundances.forEach(abundance => {
                    let promise = this.getEcoCodesForTaxon(abundance.taxon_id);
                    fetchPromises.push(promise);
                    promise.then(ecoCodes => {
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
            this.saveEcocodeBundlesToCache(bundleObject);
        }
        
        return bundleObject;
    }

    async getEcocodeBundlesFromCache(siteId) {
        let ecoCodeBundle = null;
        if(this.app.cacheStorageMethod == "mongo") {
            let findResult = await this.app.mongo.collection('site_ecocode_bundles').find({
                site_id: parseInt(siteId)
            }).toArray();
            if(findResult.length > 0) {
                ecoCodeBundle = findResult[0];
            }
        }

        if(this.app.cacheStorageMethod == "file") {
            try {
                ecoCodeBundle = fs.readFileSync("ecocode_cache/ecocode_site_"+siteId+".json");
                if(ecoCodeBundle) {
                    ecoCodeBundle = JSON.parse(ecoCodeBundle);
                }
            }
            catch(error) {
                //Nope, no such file
            }
        }

        //Check that this cached version of the site was generated by the same server version as we are running
        //If not, it will be re-generated using the (hopefully) newer version
        if(ecoCodeBundle && ecoCodeBundle.api_source == this.app.appName+"-"+this.app.appVersion) {
            return ecoCodeBundle;
        }
        return false;
    }

    async saveEcocodeBundlesToCache(ecoCodeBundle) {
        if(this.app.cacheStorageMethod == "mongo") {
            await this.app.mongo.collection('site_ecocode_bundles').deleteOne({ site_id: parseInt(ecoCodeBundle.site_id) });
            this.app.mongo.collection('site_ecocode_bundles').insertOne(ecoCodeBundle);
        }
        if(this.app.cacheStorageMethod == "file") {
            fs.writeFileSync("ecocode_cache/ecocode_site_"+ecoCodeBundle.site_id+".json", JSON.stringify(ecoCodeBundle, null, 2));
        }
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