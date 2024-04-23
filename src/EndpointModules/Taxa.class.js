import crypto from 'crypto';

class Taxa {
    constructor(app) {
        this.app = app;

        this.app.expressApp.post('/graphs/toptaxa', async (req, res) => {
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

            let taxaList = await this.fetchTaxaForSites(siteIds, 20);
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
                                //Check if we already have this site in the list
                                let foundSite = false;
                                for(let key in abundances) {
                                    if(abundances[key].site_id == site.site_id) {
                                        foundSite = true;
                                        abundances[key].abundance += abundance.abundance;
                                    }
                                }

                                if(foundSite === false) {
                                    abundances.push({
                                        site_id: site.site_id,
                                        site_name: site.site_name,
                                        lng: site.longitude_dd,
                                        lat: site.latitude_dd,
                                        abundance: abundance.abundance
                                    });
                                }
                            }
                        });
                    });
                }
            });
        });
        
        return abundances;
    }

    async fetchTaxaForSites(siteIds, limit = 10) {
        //console.log("Fetching taxa for sites", siteIds);

        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('toptaxa'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };

        if(this.app.useTaxaSummaryCacheing === true) {
            let cachedResult = await this.app.getObjectFromCache("graph_cache", identifierObject);
            if(cachedResult !== false) {
                return cachedResult.data;
            }
        }

        let query = {
            $and: [
                { site_id: { $in : siteIds }},
                { "datasets.method_id": 3 },
            ]
        }
        
        const aggregationPipeline = [
            { $match: query },
            { $unwind: "$datasets" },
            { $match: { "datasets.method_id": 3 } },
            { $unwind: "$datasets.analysis_entities" },
            { $unwind: "$datasets.analysis_entities.abundances" },
            {
              $group: {
                _id: "$datasets.analysis_entities.abundances.taxon_id",
                abundance: { $sum: "$datasets.analysis_entities.abundances.abundance" },
              },
            },
            { $project: { _id: 0, taxon_id: "$_id", abundance: 1 } },
        ];
        
        let taxaList = await this.app.mongo.collection("sites").aggregate(aggregationPipeline).toArray();

        //sort by abundance
        taxaList.sort((a, b) => {
            return b.abundance - a.abundance;
        });

        if(limit) {
            taxaList = taxaList.slice(0, limit);
        }

        //Also fetch the names for these species
        for(let key in taxaList) {
            let taxon = await this.app.getObjectFromCache("taxa", { taxon_id: taxaList[key].taxon_id}, true);
            if(taxon && taxon.length > 0) {
                taxaList[key].family = taxon[0].family.family_name;
                taxaList[key].genus = taxon[0].genus.genus_name;
                taxaList[key].species = taxon[0].species;
            }
        }
        
        let resultObject = {
            cache_id: cacheId,
            data: taxaList
        }

        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
        
        return taxaList;
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

export default Taxa;