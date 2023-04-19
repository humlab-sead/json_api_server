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

        this.app.expressApp.get('/taxa/preload/:flushCache?', async (req, res) => {

        });

        this.app.expressApp.get('/taxon_distribution/:taxon_id', async (req, res) => {
            this.fetchDistributionForTaxon(req.params.taxon_id).then(distribution => {
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(distribution, null, 2));
            });
        });

    }

    fetchDistributionForAllTaxa() {
        
    }

    async fetchDistributionForTaxon(taxonId) {
        taxonId = parseInt(taxonId);

        let sites = await this.app.mongo.collection("sites").find({ "datasets.analysis_entities.abundances.taxon_id": taxonId }).toArray();

        let abundances = [];
        sites.forEach(site => {
            site.datasets.forEach(dataset => {
                if(dataset.method_id == 3) { //this is an abundance counting dataset
                    dataset.analysis_entities.forEach(ae => {
                        ae.abundances.forEach(abundance => {
                            if(abundance.taxon_id == taxonId) {
                                abundances.push({
                                    site_id: site.site_id,
                                    site_name: site.site_name,
                                    lng: site.longitude_dd,
                                    lat: site.latitude_dd,
                                    abundance: abundance.abundance
                                });
                            }
                        });
                    });
                }
            });
        });
        
        return abundances;
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

        let taxonIds = [];
        let taxonList = [];

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
                                taxonIds.push(abundance.taxon_id);
                            }
                        });
                    });
                }
            });
        });
        
        //Also fetch the names for these species
        let taxa = [];
        let taxaObjects = await this.app.getObjectFromCache("taxa", { taxon_id: { $in : taxonIds }}, true);

        taxonList.forEach(taxonListItem => {
            let taxonData = this.getTaxonById(taxaObjects, taxonListItem.taxonId);
            taxa.push({
                taxonId: taxonListItem.taxonId,
                abundance: taxonListItem.abundance,
                family: taxonData.family.family_name,
                genus: taxonData.genus.genus_name,
                species: taxonData.species
            });
        });

        this.app.saveObjectToCache("site_taxa_cache", identifierObject, { cache_id: cacheId, taxa: taxa });
        
        return taxa;
    }

    getTaxonById(taxa, taxonId) {
        for(let key in taxa) {
            if(taxa[key].taxon_id == taxonId) {
                return taxa[key];
            }
        }
        return false;
    }
}

module.exports = Taxa;