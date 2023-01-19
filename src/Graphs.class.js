const crypto = require('crypto');

class Graphs {
    constructor(app) {
        this.app = app;

        this.app.expressApp.post('/graphs/analysis_methods', async (req, res) => {
            let siteIds = req.body;
            console.log(siteIds);
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
            res.send(JSON.stringify(analysisMethods, null, 2));
        });
    }

    async fetchAnalysisMethodsSummaryForSites(sites) {
        let cacheId = crypto.createHash('sha256');
        cacheId = cacheId.update('analysismethods'+JSON.stringify(sites)).digest('hex');
        let identifierObject = { cache_id: cacheId };

        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if(cachedData !== false) {
            return cachedData.data;
        }

        let query = {
            lookup_tables: {
                analysis_methods: {
                    method_id: 8
                }
            }
        }

        console.log(this.app, this.app.mongo);
        let findResult = await this.app.mongo.collection('sites').find(query).toArray();

        console.log(findResult);

        /*
        //If we get this far, the cache search was a miss, so we need to perform an SQL fetch

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = ` SELECT sample_groups.site_id,
        sample_groups.sample_group_id,
        methods.method_id,
        methods.method_name,
        methods.method_abbrev_or_alt_name
        FROM postgrest_api.sample_groups
         LEFT JOIN postgrest_api.physical_samples ON sample_groups.sample_group_id = physical_samples.sample_group_id
         LEFT JOIN postgrest_api.analysis_entities ON physical_samples.physical_sample_id = analysis_entities.physical_sample_id
         LEFT JOIN postgrest_api.datasets ON analysis_entities.dataset_id = datasets.dataset_id
         LEFT JOIN postgrest_api.methods ON datasets.method_id = methods.method_id;`;

        this.app.pgClient.query(sql, [taxon.taxa_seasonality[key].location_id]);
        */

        /*
        [
            {
                analysis_method_id: 0,
                count: 0,
            }
        ]
        */
    }
    
}

module.exports = Graphs;