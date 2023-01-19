const crypto = require('crypto');

class Graphs {
    constructor(app) {
        this.app = app;

        this.app.expressApp.post('/graphs/analysis_methods', async (req, res) => {
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

            let analysisMethods = await this.fetchAnalysisMethodsSummaryForSites(siteIds);
            res.header("Content-type", "application/json");
            res.end(JSON.stringify(analysisMethods, null, 2));
        });

        this.app.expressApp.post('/graphs/sample_methods', async (req, res) => {
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

            let data = await this.fetchSampleMethodsSummaryForSites(siteIds);
            res.header("Content-type", "application/json");
            res.end(JSON.stringify(data, null, 2));
        });
    }

    async fetchSampleMethodsSummaryForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        cacheId = cacheId.update('samplemethods'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };

        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if(cachedData !== false) {
            return cachedData;
        }

        let query = { site_id : { $in : siteIds } };
        /*
        let query = {
            $and: [
                { site_id: { $in : siteIds }},
                { "sample_groups.sampling_context.descriptions.sample_description_type_id": 30 }
                //{ "sample_groups.physical_samples.descriptions.sample_description_type_id": 30 }
            ]
            
        };
        */

        let sites = await this.app.mongo.collection('sites').find(query).toArray();
        let methods = [];
        sites.forEach(site => {
            site.sample_groups.forEach(sg => {

                let foundMethod = false;
                for(let key in methods) {
                    if(methods[key].method_id == sg.method_id) {
                        foundMethod = true;
                        methods[key].sample_groups_count++;
                    }
                }
                if(!foundMethod) {
                    let methodMeta = null;
                    for(let key in site.lookup_tables.analysis_methods) {
                        if(site.lookup_tables.sample_methods[key].method_id == sg.method_id) {
                            methodMeta = site.lookup_tables.sample_methods[key];
                        }
                    }

                    methodMeta.sample_groups_count = 1;
                    methods.push(methodMeta);
                }
            })
        });

        let resultObject = {
            cache_id: cacheId,
            sample_methods_sample_groups: methods
        }

        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);

        return resultObject;
    }

    async fetchAnalysisMethodsSummaryForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        cacheId = cacheId.update('analysismethods'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };

        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if(cachedData !== false) {
            return cachedData;
        }
        
        let query = { site_id : { $in : siteIds } };
        let sites = await this.app.mongo.collection('sites').find(query).toArray();

        let methods = [];
        sites.forEach(site => {
            site.datasets.forEach(dataset => {

                let foundMethod = false;
                for(let key in methods) {
                    if(methods[key].method_id == dataset.method_id) {
                        foundMethod = true;
                        methods[key].dataset_count++;
                    }
                }
                if(!foundMethod) {
                    let methodMeta = null;
                    for(let key in site.lookup_tables.analysis_methods) {
                        if(site.lookup_tables.analysis_methods[key].method_id == dataset.method_id) {
                            methodMeta = site.lookup_tables.analysis_methods[key];
                        }
                    }

                    methodMeta.dataset_count = 1;
                    methods.push(methodMeta);
                }
            })
        });

        let resultObject = {
            cache_id: cacheId,
            analysis_methods_datasets: methods
        }

        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);

        return resultObject;
    }
    
}

module.exports = Graphs;