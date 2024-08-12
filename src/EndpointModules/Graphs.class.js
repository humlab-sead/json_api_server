import crypto from 'crypto';

class Graphs {
    constructor(app) {
        this.app = app;

        //add a graph endpoint for rendering an overview of domain data for a site
        this.app.expressApp.post('/graphs/site/domains_overview', async (req, res) => {
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

          let data = await this.fetchDomainsOverviewForSites(siteIds);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(data, null, 2));
        });

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

        this.app.expressApp.post('/graphs/feature_types', async (req, res) => {
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

          let data = await this.fetchFeatureTypesSummaryForSites(siteIds);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(data, null, 2));
        });

        this.app.expressApp.post('/graphs/dating_overview', async (req, res) => {
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

          let data = await this.fetchDatingOverviewForSites(siteIds);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(data, null, 2));
        });

        this.app.expressApp.post('/graphs/dynamic_chart', async (req, res) => {
          let siteIds = req.body.siteIds;
          req.body.x;
          req.body.y;
          req.body.variable;
          req.body.groupBy;

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

          let data = await this.fetchDynamicChart(req.body);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(data, null, 2));
        });
        
        this.app.expressApp.post('/graphs/grouped_data_by_variable', async (req, res) => {
          let siteIds = req.body.siteIds;
          let variableName = req.body.variableName;
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

          if(!variableName) {
            res.status(400);
            res.send("Bad input - should include a variable name");
            return;
          }

          let data = await this.fetchGroupedDataByVariable(siteIds, variableName);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(data, null, 2));
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
      siteIds.sort((a, b) => a - b);
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

    async fetchDynamicChart(requestData) {
        if(requestData.chartType === "pie") {
          return await this.fetchPieChart(requestData.siteIds, requestData.variable, requestData.groupBy);
        }
        if(requestData.chartType === "bar") {
          return await this.fetchBarChart(requestData.siteIds, requestData.x, requestData.y);
        }
    }

    async fetchPieChart(siteIds, variable, groupBy) {
      /* variable possibilities:
      analysis_methods
      dataset_count
      dataset_type

      groupBy possibilities:
      dataset_count
      time
      */

      const varDefs = [];
      varDefs.push({
        varName: "analysis_methods",
        variablePath: "$datasets.method_id",
        lookupPath: "$lookup_tables.methods"
      });

      const groupByDefs = [];
      groupByDefs.push({
        varName: "dataset_count",
        groupByPath: "$datasets.method_id"
      });


      let selectedVariable = varDefs.find(v => v.varName === variable);
      let selectedGroupBy = groupByDefs.find(v => v.varName === groupBy);

      //construct a mongodb pipeline
      let pipeline = [
        { $match: { site_id: { $in: siteIds } } },
        { $unwind: "$datasets" },
        { $unwind: selectedVariable.lookupPath },
        {
          $group: {
            _id: selectedVariable.variablePath,
            count: { $sum: 1 }
          }
        },
        {
          $project: {
            _id: 0,
            value: "$_id",
            count: 1
          }
        }
      ];
    }

    async fetchBarChart(siteIds, x, y) {
    
    }

    /**
     * fetchGroupedDataByVariable
     * @param {*} siteIds array of siteIds
     * @param {*} variableName a predefined variable name, e.g. "sampleFeatures"
     * @returns 
     */
    async fetchGroupedDataByVariable(siteIds, variableName) {
      let cacheId = crypto.createHash('sha256');
      siteIds.sort((a, b) => a - b);
      cacheId = cacheId.update(variableName + JSON.stringify(siteIds)).digest('hex');
      let identifierObject = { cache_id: cacheId };
      let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
      if (cachedData !== false && this.app.useGraphCaching) {
        return cachedData;
      }

      let searchableVariables = [];
      searchableVariables.push({
        varName: "sampleFeatures",
        variablePath: "$sample_groups.physical_samples.features",
        groupByPath: "$sample_groups.physical_samples.features.feature_type_id",
        key: "feature_type_id"
      });
      searchableVariables.push({
        varName: "sampleMethods",
        variablePath: "$sample_groups.method_id",
        groupByPath: "$sample_groups.method_id",
        key: "method_id"
      });


      //varaiable definition for getting an overview of the existance of wayney edges in the dendro data
      /*
      searchableVariables.push({
        varName: "dendroWayneyEdges",
        variablePath: "$datagroups.datasets.id", // == 128
        groupByPath: "$datagroups.datasets.value",
        key: "dating_method_id"
      });
      */
      /*
      let dendroWayneyEdgesPipeline = {
        $match: { site_id: { $in: siteIds } },
        $unwind: "$datagroups",
        $unwind: "$datagroups.datasets",
        $match: { "datagroups.datasets.id": 128 },
        $group: {
          _id: "$datagroups.datasets.value",
          count: { $sum: 1 }
        },
        $project: {
          _id: 0,
          value: "$_id",
          count: 1
        }
      };
      */


      let variablePath = searchableVariables.find(v => v.varName === variableName).variablePath;
      let groupByPath = searchableVariables.find(v => v.varName === variableName).groupByPath;
      let key = searchableVariables.find(v => v.varName === variableName).key;

     // Initialize the pipeline with the match stage
      let pipeline = [{ $match: { site_id: { $in: siteIds } } }];

      // Dynamically add $unwind stages for the specified path
      let pathParts = variablePath.slice(1).split('.'); // Remove the leading '$'
      let unwindPath = '';

      //create unwind stages
      pathParts.forEach((part, index) => {
        // Reconstruct the unwind path incrementally
        unwindPath += (index === 0 ? '' : '.') + part;
        pipeline.push({ $unwind: `$${unwindPath}` });
      });

      //create mach stages

      // Append group and project stages to the pipeline
      pipeline.push(
        { $group: {
          _id: groupByPath,
          count: { $sum: 1 }
        } },
        { $project: {
          _id: 0,
          value: "$_id",
          count: 1
        } }
      );

      console.log(pipeline);
      
      let data = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
      let resultObject = {
        cache_id: cacheId,
        data: data,
        meta: {
          variableName: variableName,
          key: key
        }
      };

      if(this.app.useGraphCaching) {
        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
      }
      console.log(resultObject);
      return resultObject;
    }

    async fetchDatingOverviewForSites(siteIds) {
      let cacheId = crypto.createHash('sha256');
      siteIds.sort((a, b) => a - b);
      cacheId = cacheId.update('dating_extremes' + JSON.stringify(siteIds)).digest('hex');
      let identifierObject = { cache_id: cacheId };
    
      let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
      if (cachedData !== false) {
        return cachedData;
      }
    
      let pipeline = [
        { $match: { site_id: { $in: siteIds } } },
        {
          $project: {
              site_id: 1,
              age_older: '$chronology_extremes.age_older',
              age_younger: '$chronology_extremes.age_younger'
          }
      }
      ];
    
      let dating_extremes = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
    
      let resultObject = {
        cache_id: cacheId,
        dating_extremes: dating_extremes
      };
    
      this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
    
      return resultObject;
    }

    async fetchFeatureTypesSummaryForSites(siteIds) {
      let cacheId = crypto.createHash('sha256');
      siteIds.sort((a, b) => a - b);
      cacheId = cacheId.update('featuretypes' + JSON.stringify(siteIds)).digest('hex');
      let identifierObject = { cache_id: cacheId };
    
      //let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
      let cachedData = false;
      if (cachedData !== false) {
        return cachedData;
      }
    
      let pipeline = [
        { $match: { site_id: { $in: siteIds } } },
        { $unwind: "$sample_groups" },
        { $unwind: "$sample_groups.physical_samples" },
        { $unwind: "$sample_groups.physical_samples.features" },
        {
          $group: {
            _id: "$sample_groups.physical_samples.features.feature_type_id",
            name: { $first: "$sample_groups.physical_samples.features.feature_type_name" },
            feature_count: { $sum: 1 }
          }
        },
        {
          $project: {
            feature_type: "$_id",
            name: 1,
            _id: 0,
            feature_count: 1
          }
        },
        {
          $sort: { feature_count: -1 }  // Sort by feature_count in descending order
        }
      ];
    
      let featureTypes = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
    
      let resultObject = {
        cache_id: cacheId,
        feature_types: featureTypes
      };
    
      this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
      
      return resultObject;
    }

    async fetchSampleMethodsSummaryForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
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
                      input: "$lookup_tables.methods",
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
        siteIds.sort((a, b) => a - b);
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

    async fetchDatasetOverviewForSites(siteIds) {
        
    }

    //WARNING: this function does seem to work, but not all datasets are covered by domains (such as dating methods), so the result is less useful than I would like since it doesn't do a very good job of showing an overview of what data is available in a site
    async fetchDomainsOverviewForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('domains'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) {
          return cachedData;
        }

        //let's compile a summary of the domains covered by this site and how many analysis_entities are in each domain
        //we do this by matching $datasets.method_id with the method_ids in the lookup_tables.domains
      
        let pipeline = [
          { $match: { site_id: { $in: siteIds } } },
          { $unwind: "$datasets" },
          { $unwind: "$lookup_tables.domains" },
          {
            $match: {
              "$expr": {
                "$in": ["$datasets.method_id", "$lookup_tables.domains.method_ids"]
              }
            }
          },
          {
            $group: {
              _id: "$lookup_tables.domains.facet_code",
              dataset_count: { $sum: 1 },
              domain_meta: { $first: "$lookup_tables.domains" }
            }
          },
          {
            $project: {
              method_id: "$_id",
              _id: 0,
              dataset_count: 1,
              domain_meta: 1
            }
          }
        ];
        
        let sites = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();

        let domains = sites.map(site => ({
          ...site.domain_meta,
          dataset_count: site.dataset_count
        }));
      
        let resultObject = {
          cache_id: cacheId,
          domains: domains
        };
      
        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
        return resultObject;
    }

    async flushGraphCache() {
      console.log("Flushing graph cache");
      await this.app.mongo.collection('graph_cache').deleteMany({});
    }

    async fetchFeatureTypesSummaryForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('featuretypes'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        if(this.app.useGraphCaching) {
          let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
          if (cachedData !== false) {
            return cachedData;
          }
        }
      
        let pipeline = [
          { $match: { site_id: { $in: siteIds } } },
          { $unwind: "$sample_groups" },
          { $unwind: "$sample_groups.physical_samples" },
          { $unwind: "$sample_groups.physical_samples.features" },
          {
            $group: {
              _id: "$sample_groups.physical_samples.features.feature_type_id",
              name: { $first: "$sample_groups.physical_samples.features.feature_type_name" },
              feature_count: { $sum: 1 }
            }
          },
          {
            $project: {
              feature_type: "$_id",
              name: 1,
              _id: 0,
              feature_count: 1
            }
          }
        ];
      
        let featureTypes = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
      
        let resultObject = {
          cache_id: cacheId,
          feature_types: featureTypes
        };
      
        if(this.app.useGraphCaching) {
          this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
        }
      
        return resultObject;
    }

    async fetchAnalysisMethodsSummaryForSites(siteIds) {
        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('analysismethods'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        if(this.app.useGraphCaching) {
          let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
          if (cachedData !== false) {
            return cachedData;
          }
        }
        
        let pipeline = [
          { $match: { site_id: { $in: siteIds } } },
          // Ensure lookup_tables.methods is accessible after unwinding
          {
            $addFields: {
              "datasets.lookup_methods": "$lookup_tables.methods"
            }
          },
          { $unwind: "$datasets" },
          {
            $group: {
              _id: "$datasets.method_id",
              dataset_count: { $sum: 1 },
              method_meta: {
                $first: {
                  $filter: {
                    input: "$datasets.lookup_methods", // Adjusted to the added field
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
      
        if(this.app.useGraphCaching) {
          this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
        }
      
        return resultObject;
    }
    
}

export default Graphs;