const crypto = require('crypto');

class Taxa {
    constructor(app) {
        this.app = app;

        this.app.expressApp.post('/taxa', async (req, res) => {
            let siteIds = req.body;
            //siteIds = JSON.parse(siteIds);
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

            let taxaList = await this.fetchTaxaForSites(siteIds);
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(taxaList, null, 2));
        });

    }

    async fetchTaxaForSites(sites) {
        let cacheId = crypto.createHash('sha256');
        cacheId = cacheId.update(JSON.stringify(sites)).digest('hex');
        let identifierObject = { cache_id: cacheId };

        let taxaCached = await this.app.getObjectFromCache("site_taxa_cache", identifierObject);
        if(taxaCached !== false) {
            return taxaCached.taxa;
        }

        let taxonList = [];

        let sitePromises = [];
        sites.forEach(siteId => {
            let sitePromise = this.app.getObjectFromCache("sites", { site_id: siteId });
            sitePromises.push(sitePromise);
            sitePromise.then(site => {
                if(site) {
                    site.datasets.forEach(dataset => {
                        if(dataset.method_id == 3) { //this is an abundance counting dataset
                            dataset.analysis_entities.forEach(ae => {
                                ae.abundances.forEach(abundance => {
                                    let foundTaxon = false;
                                    for(let key in taxonList) {
                                        if(taxonList[key].taxonId == abundance.taxon_id) {
                                            foundTaxon = true;
                                            taxonList[key].abundance += abundance.abundance;
                                        }
                                    }
                                    if(foundTaxon === false) {
                                        taxonList.push({
                                            taxonId: abundance.taxon_id,
                                            abundance: abundance.abundance
                                        });
                                    }
                                });
                            });
                        }
                    });
                }
            });
        });

        await Promise.all(sitePromises);
        
        //Also fetch the names for these species
        let taxaPromises = [];
        let taxa = [];
        taxonList.forEach(taxon => {
            let taxonPromise = this.app.getObjectFromCache("taxa", { taxon_id: taxon.taxonId });
            taxaPromises.push(taxonPromise);
            taxonPromise.then(taxonData => {
                if(taxonData) {
                    taxa.push({
                        taxonId: taxon.taxonId,
                        abundance: taxon.abundance,
                        family: taxonData.family.family_name,
                        genus: taxonData.genus.genus_name,
                        species: taxonData.species
                    });
                }
            });
        });

        await Promise.all(taxaPromises);

        this.app.saveObjectToCache("site_taxa_cache", identifierObject, { cache_id: cacheId, taxa: taxa });
        
        return taxa;
    }
}

module.exports = Taxa;