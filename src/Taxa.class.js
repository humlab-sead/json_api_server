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

    //This function is entirely untested - it hasn't been run even once, at all, so have fun with that! :)
    async fetchTaxaForSites(sites) {
        console.log("figuring out taxa for:")
        console.log(sites);

        let taxonList = [];

        sites.forEach(siteId => {
            let site = this.app.getObjectFromCache("sites", { site_id: siteId });
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

        //Make them unique
        taxonList = taxonList.filter((value, index, self) => {
            return self.indexOf(value) === index;
        });

        return taxonList;
    }
}

module.exports = Taxa;