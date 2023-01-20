const crypto = require('crypto');

class Taxa {
    constructor(app) {
        this.app = app;

        this.app.expressApp.post('/taxa', async (req, res) => {
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

            let taxaList = await this.fetchTaxaForSites(siteIds);
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(taxaList, null, 2));
        });

    }

    async fetchTaxaForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        cacheId = cacheId.update(JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };

        let taxaCached = await this.app.getObjectFromCache("site_taxa_cache", identifierObject);
        if(taxaCached !== false) {
            return taxaCached.taxa;
        }

        let query = {
            $and: [
                { site_id: { $in : siteIds }},
                { "datasets.method_id": 3 },
            ]
        }
        let sites = await this.app.getObjectFromCache("sites", query, true);

        let taxonList = [];

        let sitePromises = [];
        sites.forEach(site => {
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
                else {
                    console.warn("Failed fetching taxon, no entry for taxon_id "+taxon.taxonId);
                }
            });
        });

        await Promise.all(taxaPromises);

        this.app.saveObjectToCache("site_taxa_cache", identifierObject, { cache_id: cacheId, taxa: taxa });
        
        return taxa;
    }
}

module.exports = Taxa;