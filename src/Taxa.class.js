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

    //This function is entirely untested - it hasn't been run even once, at all, so have fun with that! :)
    async fetchTaxaForSites(sites) {
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
                                    taxonList.push(abundance.taxon_id);
                                });
                            });
                        }
                    });
                }
            });
        });

        await Promise.all(sitePromises);

        //Make them unique
        taxonList = taxonList.filter((value, index, self) => {
            return self.indexOf(value) === index;
        });
        
        //Also fetch the names for these species
        let taxaPromises = [];
        let taxa = [];
        taxonList.forEach(taxonId => {
            let taxonPromise = this.app.getObjectFromCache("taxa", { taxon_id: taxonId });
            taxaPromises.push(taxonPromise);
            taxonPromise.then(taxon => {
                if(taxon) {
                    taxa.push({
                        taxonId: taxonId,
                        family: taxon.family.family_name,
                        genus: taxon.genus.genus_name,
                        species: taxon.species
                    });
                }
            });
        });

        await Promise.all(taxaPromises);
        
        return taxa;
    }
}

module.exports = Taxa;