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

        this.app.expressApp.post('/graphs/temporal_distributon', async (req, res) => {
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

            let chartData = await this.fetchTemporalDistributionSummaryForSites(siteIds);
            res.header("Content-type", "application/json");
            res.end(JSON.stringify(chartData, null, 2));
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

        this.app.expressApp.post('/graphs/ecocodes', async (req, res) => {
          let siteIds = req.body;
          if(typeof siteIds != "object") {
              res.status(400);
              res.send("Bad input - should be an array of site IDs");
              return;
          }

          this.fetchEcocodesSummaryForSites(siteIds).then(data => {
            res.header("Content-type", "application/json");
            res.end(JSON.stringify(data, null, 2));
          });

        });
    }

    async fetchEcocodesSummaryForSites(siteIds) {
      let cacheId = crypto.createHash('sha256');
      cacheId = cacheId.update('ecocodes' + JSON.stringify(siteIds)).digest('hex');
      let identifierObject = { cache_id: cacheId };
      
      let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
      if(cachedData !== false) {
        return cachedData;
      }
      
      let pipeline = [
        { $match: { site_id: { $in: siteIds } } },
        { $unwind: "$ecocode_bundles" },
        {
          $group: {
            _id: "$ecocode_bundles.ecocode.abbreviation",
            totalAbundance: { $sum: "$ecocode_bundles.abundance" },
            ecocodeDefinitionId: { $first: "$ecocode_bundles.ecocode.ecocode_definition_id" },
            ecocodeName: { $first: "$ecocode_bundles.ecocode.name" }
          }
        },
        {
          $group: {
            _id: "$ecocodeDefinitionId",
            ecocodes: {
              $push: {
                ecocode_definition_id: "$ecocodeDefinitionId",
                abbreviation: "$_id",
                name: "$ecocodeName",
                totalAbundance: "$totalAbundance"
              }
            }
          }
        }
      ];
    
      let ecocodesGroups = await this.app.mongo.collection('site_ecocode_bundles').aggregate(pipeline).toArray();
      
      let ecocodes = [];
      ecocodesGroups.forEach(group => {
        ecocodes.push({
          ecocode_definition_id: group._id,
          abbreviation: group.ecocodes[0].abbreviation,
          name: group.ecocodes[0].name,
          totalAbundance: group.ecocodes[0].totalAbundance
        });
      });

      let resultObject = {
        cache_id: cacheId,
        ecocode_groups: ecocodes
      };
    
      this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
    
      return resultObject;
    }
    
    async fetchSampleMethodsSummaryForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        cacheId = cacheId.update('samplemethods' + JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) {
          return cachedData;
        }

        let pipeline = [
            { $match: { site_id: { $in: siteIds } } },
            { $unwind: "$sample_groups" },
            {
              $group: {
                _id: "$sample_groups.method_id",
                sample_groups_count: { $sum: 1 },
                method_meta: {
                  $first: {
                    $filter: {
                      input: "$lookup_tables.sampling_methods",
                      cond: { $eq: ["$$this.method_id", "$sample_groups.method_id"] }
                    }
                  }
                }
              }
            },
            {
              $project: {
                method_id: "$_id",
                _id: 0,
                sample_groups_count: 1,
                method_meta: { $arrayElemAt: ["$method_meta", 0] }
              }
            }
        ];
      
        let methods = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
      
        let resultObject = {
          cache_id: cacheId,
          sample_methods_sample_groups: methods
        };

        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
      
        return resultObject;
    }

    async fetchTemporalDistributionSummaryForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        cacheId = cacheId.update('analysismethods'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };

        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if(cachedData !== false) {
            return cachedData;
        }


        let datingMethods = [];

        let query = { site_id : { $in : siteIds } };
        let sites = await this.app.mongo.collection('sites').find(query).toArray();
        sites.forEach(site => {
            site.datasets.forEach(dataset => {
                //find out if this is a dataset with dating_values
                if(datingMethods.includes(dataset.method_id)) {
                    dataset.analysis_entities.forEach(ae => {
                        ae.dating_values
                    });
                }

            });
        });

        let resultObject = {
            cache_id: cacheId,
            datasets: []
        }
        return resultObject;
    }

    async fetchAnalysisMethodsSummaryForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        cacheId = cacheId.update('analysismethods'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) {
          return cachedData;
        }
      
        let pipeline = [
          { $match: { site_id: { $in: siteIds } } },
          { $unwind: "$datasets" },
          {
            $group: {
              _id: "$datasets.method_id",
              dataset_count: { $sum: 1 },
              method_meta: {
                $first: {
                  $filter: {
                    input: "$lookup_tables.analysis_methods",
                    cond: { $eq: ["$$this.method_id", "$datasets.method_id"] }
                  }
                }
              }
            }
          },
          {
            $project: {
              method_id: "$_id",
              _id: 0,
              dataset_count: 1,
              method_meta: { $arrayElemAt: ["$method_meta", 0] }
            }
          }
        ];
      
        let sites = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
      
        let methods = sites.map(site => ({
          ...site.method_meta,
          dataset_count: site.dataset_count
        }));
      
        let resultObject = {
          cache_id: cacheId,
          analysis_methods_datasets: methods
        };
      
        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
      
        return resultObject;
    }
    
}

module.exports = Graphs;