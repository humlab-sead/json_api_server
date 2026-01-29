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

        this.app.expressApp.post('/graphs/datings', async (req, res) => {
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

          let datings = await this.fetchDatingsSummaryForSites(siteIds);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(datings, null, 2));
        });

        
        this.app.expressApp.post('/graphs/site/domain_sample_summary', async (req, res) => {
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
          let data = await this.fetchDomainSampleSummaryForSites(siteIds);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(data, null, 2));
        });
        
        this.app.expressApp.post('/graphs/data_count', async (req, res) => {
          let siteIds = req.body.siteIds;
          let mongoPath = req.body.path;
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

          if(!mongoPath) {
            res.status(400);
            res.send("Bad input - should include a path");
            return;
          }

          let data = await this.fetchItemCountForSites(siteIds, mongoPath);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(data, null, 2));
        });


        this.app.expressApp.post('/graphs/custom/:perSite?', async (req, res) => {
          let siteIds = req.body.siteIds;
          let mongoPath = req.body.path;
          let idField = req.body.idField;
          let nameField = req.body.nameField;
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

          if(!mongoPath) {
            res.status(400);
            res.send("Bad input - should include a path");
            return;
          }

          if(!idField) {
            res.status(400);
            res.send("Bad input - should include an id field");
            return;
          }

          if(!nameField) {
            res.status(400);
            res.send("Bad input - should include a name field");
            return;
          }

          let data = [];
          if(req.params.perSite === "true") {
            let promises = [];
            siteIds.forEach(async siteId => {
              //promises.push(this.fetchFeatureTypesSummaryForSites([siteId]));
              promises.push(this.fetchSummaryForSites([siteId], mongoPath, idField, nameField));
            });

            let siteData = await Promise.all(promises);
            siteData.forEach(site => {
              data.push({
                site_id: site.siteIds[0],
                summary_data: site.summary_data
              });
            });
          }
          else {
            //data = await this.fetchFeatureTypesSummaryForSites(siteIds);
            data = await this.fetchSummaryForSites(siteIds, mongoPath, idField, nameField);
          }

          //let data = await this.fetchSummaryForSites(siteIds, mongoPath, idField, nameField);
          res.header("Content-type", "application/json");
          res.end(JSON.stringify(data, null, 2));
        });


        this.app.expressApp.post('/graphs/feature_types/:perSite?', async (req, res) => {
          let siteIds = req.body.siteIds;
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

          let data = [];
          if(req.params.perSite === "true") {
            let promises = [];
            siteIds.forEach(async siteId => {
              //promises.push(this.fetchFeatureTypesSummaryForSites([siteId]));
              promises.push(this.fetchSummaryForSites([siteId], "sample_groups.physical_samples.features", "feature_type_id", "feature_type_name"));
            });

            let siteData = await Promise.all(promises);
            siteData.forEach(site => {
              data.push({
                site_id: site.siteIds[0],
                feature_types: site.summary_data
              });
            });
          }
          else {
            //data = await this.fetchFeatureTypesSummaryForSites(siteIds);
            data = await this.fetchSummaryForSites(siteIds, "sample_groups.physical_samples.features", "feature_type_id", "feature_type_name");
          }
          
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

    async fetchFeatureTypesSummaryForSitesNEW(siteIds) {
      let cacheId = crypto.createHash('sha256');
      siteIds.sort((a, b) => a - b);
      cacheId = cacheId.update('featuretypes' + JSON.stringify(siteIds)).digest('hex');
      let identifierObject = { cache_id: cacheId };
  
      let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
      if (cachedData !== false) {
          //return cachedData;
      }
  
      // Build the pipeline
      let pipeline = [];
      
      // Only add the $match stage if specific site IDs are passed
      if (siteIds.length > 0) {
          pipeline.push({ $match: { site_id: { $in: siteIds } } });
      }
      else {
        console.log("No site IDs passed");
      }
  
      pipeline.push(
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
      );
  
      let featureTypes = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
  
      let resultObject = {
          cache_id: cacheId,
          feature_types: featureTypes
      };
  
      this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
  
      return resultObject;
  }
  

    async fetchFeatureTypesSummaryForSitesOLD_OLD(siteIds) {
      let cacheId = crypto.createHash('sha256');
      siteIds.sort((a, b) => a - b);
      cacheId = cacheId.update('featuretypes' + JSON.stringify(siteIds)).digest('hex');
      let identifierObject = { cache_id: cacheId };
    
      let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
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

    async fetchFeatureTypesSummaryForSites_OLD_FUNCTIONAL(siteIds) {
        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('featuretypes'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        if(this.app.useGraphCaching) {
          let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
          if (cachedData !== false) {
            //return cachedData;
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


    /**
     * This method will count the specified path/array. E.g. you can define a path like "sample_groups.physical_samples" and it will count the number of samples in the specified path.
     * 
     * @param {*} siteIds 
     * @param {*} mongoPath 
     * @returns 
     */
    async fetchItemCountForSites(siteIds, mongoPath) {
      let cacheId = crypto.createHash('sha256');
  
      // Sort site IDs and generate cache ID
      siteIds.sort((a, b) => a - b);
      cacheId = cacheId.update(`count_${mongoPath}${JSON.stringify(siteIds)}`).digest('hex');
      let identifierObject = { cache_id: cacheId };
  
      // If graph caching is enabled, check cache
      if (this.app.useGraphCaching) {
          let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
          if (cachedData !== false) {
              return cachedData;
          }
      }
  
      // Build the aggregation pipeline
      let pipeline = [];
  
      // Only add the $match stage if siteIds is not empty (special case for all sites)
      if (siteIds.length > 0) {
          pipeline.push({ $match: { site_id: { $in: siteIds } } });
      }
  
      // Dynamically construct unwind stages based on full mongoPath
      let pathSegments = mongoPath.split('.');
      let accumulatedPath = '';
  
      for (let i = 0; i < pathSegments.length; i++) {
          accumulatedPath = accumulatedPath ? `${accumulatedPath}.${pathSegments[i]}` : pathSegments[i];
          pipeline.push({ $unwind: `$${accumulatedPath}` });
      }
  
      // Group stage to count items at the specified path
      pipeline.push(
          {
              $group: {
                  _id: null, // Not grouping by id, so we can set _id to null
                  count: { $sum: 1 } // Count occurrences
              }
          },
          {
              $project: {
                  count: 1,
                  _id: 0
              }
          }
      );
  
      // Fetch data based on the constructed pipeline
      let itemCountData = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
  
      // Prepare result object
      let resultObject = {
          cache_id: cacheId,
          item_count: itemCountData.length > 0 ? itemCountData[0].count : 0,
          siteIds: siteIds
      };
  
      // Save result to cache if caching is enabled
      if (this.app.useGraphCaching) {
          this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
      }
  
      return resultObject;
  }
  
    async fetchSummaryForSites(siteIds, mongoPath, idField = "id", nameField = "name") {
      let cacheId = crypto.createHash('sha256');
  
      // Sort site IDs and generate cache ID
      siteIds.sort((a, b) => a - b);
      cacheId = cacheId.update(`${mongoPath}${JSON.stringify(siteIds)}`).digest('hex');
      let identifierObject = { cache_id: cacheId };
  
      // If graph caching is enabled, check cache
      if (this.app.useGraphCaching) {
          let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
          if (cachedData !== false) {
              return cachedData;
          }
      }
  
      // Build the aggregation pipeline
      let pipeline = [];
  
      // Only add the $match stage if siteIds is not empty (special case for all sites)
      if (siteIds.length > 0) {
          pipeline.push({ $match: { site_id: { $in: siteIds } } });
      }
  
      // Dynamically construct unwind stages based on full mongoPath
      let pathSegments = mongoPath.split('.');
      let accumulatedPath = ''; // Keeps track of the full path
  
      for (let i = 0; i < pathSegments.length; i++) {
          accumulatedPath = accumulatedPath ? `${accumulatedPath}.${pathSegments[i]}` : pathSegments[i];
          pipeline.push({ $unwind: `$${accumulatedPath}` });
      }
  
      // Group stage to summarize data using the complete path to id and name
      pipeline.push(
          {
              $group: {
                  _id: `$${accumulatedPath}.${idField}`, // Use full path to access "id"
                  name: { $first: `$${accumulatedPath}.${nameField}` }, // Use full path to access "name"
                  count: { $sum: 1 } // Count occurrences (can modify based on specific use case)
              }
          },
          {
              $project: {
                  id: '$_id',
                  name: 1,
                  count: 1,
                  _id: 0
              }
          }
      );
      
      // Fetch data based on the constructed pipeline
      let summaryData = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
  
      // Prepare result object
      let resultObject = {
          cache_id: cacheId,
          summary_data: summaryData,
          siteIds: siteIds
      };
  
      // Save result to cache if caching is enabled
      if (this.app.useGraphCaching) {
          this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
      }
  
      return resultObject;
    }

    async fetchFeatureTypesSummaryForSites(siteIds) {
      let cacheId = crypto.createHash('sha256');
      
      // Sort site IDs and generate cache ID
      siteIds.sort((a, b) => a - b);
      cacheId = cacheId.update('featuretypes' + JSON.stringify(siteIds)).digest('hex');
      let identifierObject = { cache_id: cacheId };
  
      // If graph caching is enabled, check cache
      if (this.app.useGraphCaching) {
          let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
          if (cachedData !== false) {
              return cachedData;
          }
      }
      
  
      // Build the aggregation pipeline
      let pipeline = [];
  
      // Only add the $match stage if siteIds is not empty (special case for all sites)
      if (siteIds.length > 0) {
          pipeline.push({ $match: { site_id: { $in: siteIds } } });
      }
  
      pipeline.push(
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
      );
  
      // Fetch feature types summary
      let featureTypes = await this.app.mongo.collection('sites').aggregate(pipeline).toArray();
  
      // Prepare result object
      let resultObject = {
          cache_id: cacheId,
          feature_types: featureTypes,
          siteIds: siteIds
      };
  
      // Save result to cache if caching is enabled
      if (this.app.useGraphCaching) {
          this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
      }
  
      return resultObject;
    }

    async fetchDomains() {
      // Get the list of domains and their associated methods from the postgres database
      const sql = `
        select facet.facet.facet_id, facet_code, display_title, clause from 
        facet.facet
        join facet.facet_clause on facet.facet_clause.facet_id = facet.facet.facet_id
        where facet.facet_group_id = 999
      `;
      let pgClient = await this.app.getDbConnection();
      try {
        let res = await pgClient.query(sql);
        // For each row, parse the clause column for numbers in the IN (...)
        return res.rows.map(row => {
          let methodIds = [];
          // Regex to match: in (number, number, ...)
          const match = row.clause.match(/in\s*\(([^)]+)\)/i);
          if (match) {
            // Only accept if it's a plain list of numbers (no SELECT etc)
            if (!/select/i.test(match[1])) {
              methodIds = match[1]
                .split(',')
                .map(s => parseInt(s.trim(), 10))
                .filter(n => !isNaN(n));
            }
          }
          return {
            ...row,
            method_ids: methodIds
          };
        });
      } finally {
        this.app.releaseDbConnection(pgClient);
      }
    }

    async fetchMethods() {
      const sql = `SELECT method_id, method_name FROM tbl_methods`;
      const pgClient = await this.app.getDbConnection();
      try {
        const res = await pgClient.query(sql);
        const methodMap = {};

        res.rows.forEach(row => {
          methodMap[row.method_id] = row.method_name;
        });
        return methodMap;
      } finally {
        this.app.releaseDbConnection(pgClient);
      }
    }

    async fetchDomainSampleSummaryForSites(siteIds) {
      // Get domains and their method_ids
      const domains = await this.fetchDomains();
      const methods = await this.fetchMethods();
      const sitesCol = this.app.mongo.collection('sites');
      const results = [];

      // Only fetch the fields we need: sample_groups.physical_samples.physical_sample_id and datasets.method_id, datasets.analysis_entities.physical_sample_id
      const projection = {
        site_id: 1,
        sample_groups: { physical_samples: { physical_sample_id: 1 } },
        datasets: { method_id: 1, analysis_entities: { physical_sample_id: 1 } }
      };
      // MongoDB projection syntax for nested arrays requires dot notation for full support, so use string keys
      const sites = await sitesCol.find(
        { site_id: { $in: siteIds } },
        {
          projection: {
            site_id: 1,
            "sample_groups.physical_samples.physical_sample_id": 1,
            "datasets.method_id": 1,
            "datasets.analysis_entities.physical_sample_id": 1
          }
        }
      ).toArray();

      // For each domain, count samples per method
      for (const domain of domains) {
        const { facet_id, clause, ...domainOut } = domain;
        if (!domain.method_ids || domain.method_ids.length === 0) {
          results.push({
            ...domainOut,
            sample_count: 0,
            method_counts: []
          });
          continue;
        }

        // Map: method_id -> Set of sample ids (to avoid double-counting same sample for same method in a site)
        const methodSampleMap = {};
        domain.method_ids.forEach(mid => { methodSampleMap[mid] = new Set(); });

        // Traverse all sites
        for (const site of sites) {
          // Build a lookup: physical_sample_id -> sample object
          const sampleIdSet = new Set();
          if (!site.sample_groups) continue;
          for (const sg of site.sample_groups) {
            if (!sg.physical_samples) continue;
            for (const sample of sg.physical_samples) {
              if (sample && sample.physical_sample_id != null) {
                sampleIdSet.add(sample.physical_sample_id);
              }
            }
          }

          // For each dataset, for each analysis_entity, check if its physical_sample_id is in sampleIdSet
          if (!site.datasets) continue;
          for (const dataset of site.datasets) {
            const method_id = dataset.method_id;
            if (!domain.method_ids.includes(method_id)) continue;
            if (!dataset.analysis_entities) continue;
            for (const ae of dataset.analysis_entities) {
              if (ae && ae.physical_sample_id != null && sampleIdSet.has(ae.physical_sample_id)) {
                methodSampleMap[method_id].add(ae.physical_sample_id);
              }
            }
          }
        }

        // Build method_counts array in the order of domain.method_ids
        const method_counts = domain.method_ids.map(method_id => {
          return {
            method_id,
            method_name: methods[method_id] || null,
            sample_count: methodSampleMap[method_id] ? methodSampleMap[method_id].size : 0
          };
        });
        const sample_count = method_counts.reduce((sum, m) => sum + m.sample_count, 0);
        results.push({
          ...domainOut,
          sample_count,
          method_counts
        });
      }
      return { domains: results };
    }

    /**
     * Aggregates analysis_entity_ages from sites, summarizes ages for a histogram using RANGE-BINNING
     * (each site counts in every bin it spans), and returns total site count.
     *
     * Age semantics (inclusive & flexible):
     * - If analysis_entity_ages.age exists: treat as a degenerate range [age, age]
     * - Else if age_younger + age_older exist: treat as range [age_younger, age_older]
     * - Each site is counted at most once per bin (unique sites per bin).
     *
     * Output:
     * { cache_id, histogram, total_sites, sites_with_datings }
     *
     * histogram: Array<{ bin_start, bin_end, count }>
     * - Uses half-open bins [bin_start, bin_end)
     */
    async fetchDatingsSummaryForSites(siteIds) {
      // ---- Guard / normalization ----
      const ids = Array.isArray(siteIds) ? siteIds.slice() : [];
      ids.sort((a, b) => a - b);

      // ---- Cache key ----
      const cacheKey = crypto
        .createHash("sha256")
        .update("datings_summary_rangebin_v2:" + JSON.stringify(ids))
        .digest("hex");

      const identifierObject = { cache_id: cacheKey };

      // ---- Check cache if enabled ----
      if (this.app.useGraphCaching) {
        const cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) return cachedData;
      }

      const sitesCol = this.app.mongo.collection("sites");

      // ---- Total sites in DB (global denominator) ----
      // Consider caching this separately if it becomes hot.
      const total_sites = await sitesCol.countDocuments();

      // ---- Common expression: derive younger/older from age or age_younger/age_older ----
      // If age exists: y=o=age
      // Else: y=age_younger, o=age_older
      const deriveYOProject = {
        site_id: 1,
        y_raw: {
          $cond: [
            { $ne: ["$analysis_entity_ages.age", null] },
            { $toDouble: "$analysis_entity_ages.age" },
            { $toDouble: "$analysis_entity_ages.age_younger" }
          ]
        },
        o_raw: {
          $cond: [
            { $ne: ["$analysis_entity_ages.age", null] },
            { $toDouble: "$analysis_entity_ages.age" },
            { $toDouble: "$analysis_entity_ages.age_older" }
          ]
        }
      };

      // ---- 1) Compute min/max over derived ranges (min younger, max older) ----
      const [mm] = await sitesCol
        .aggregate([
          { $match: { site_id: { $in: ids } } },
          { $unwind: "$analysis_entity_ages" },
          { $project: deriveYOProject },
          { $match: { y_raw: { $ne: null }, o_raw: { $ne: null } } },
          // Normalize flipped data just in case:
          {
            $project: {
              y: { $min: ["$y_raw", "$o_raw"] },
              o: { $max: ["$y_raw", "$o_raw"] }
            }
          },
          {
            $group: {
              _id: null,
              minAge: { $min: "$y" },
              maxAge: { $max: "$o" }
            }
          }
        ])
        .toArray();

      // If no datings found, return empty histogram quickly
      const binCount = 20;

      // We still return a stable histogram structure (optional).
      // If you prefer histogram = [] when no data, change below.
      if (!mm || !isFinite(mm.minAge) || !isFinite(mm.maxAge)) {
        const resultObject = {
          cache_id: cacheKey,
          histogram: [],
          total_sites,
          sites_with_datings: 0
        };

        if (this.app.useGraphCaching) {
          this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
        }
        return resultObject;
      }

      const minAge = mm.minAge;
      const maxAge = mm.maxAge;

      let binSize = (maxAge - minAge) / binCount;
      if (!isFinite(binSize) || binSize <= 0) binSize = 1;

      // ---- 2) Count UNIQUE SITES per bin by expanding ranges into bin indices ----
      const counts = await sitesCol
        .aggregate([
          { $match: { site_id: { $in: ids } } },
          { $unwind: "$analysis_entity_ages" },
          { $project: deriveYOProject },
          { $match: { y_raw: { $ne: null }, o_raw: { $ne: null } } },

          // Normalize
          {
            $project: {
              site_id: 1,
              y: { $min: ["$y_raw", "$o_raw"] },
              o: { $max: ["$y_raw", "$o_raw"] }
            }
          },

          // Compute start/end bin indices
          {
            $project: {
              site_id: 1,
              startIdx: { $floor: { $divide: [{ $subtract: ["$y", minAge] }, binSize] } },
              endIdx: { $floor: { $divide: [{ $subtract: ["$o", minAge] }, binSize] } }
            }
          },

          // Clamp indices to [0, binCount-1]
          {
            $project: {
              site_id: 1,
              startIdx: { $max: [0, "$startIdx"] },
              endIdx: { $min: [binCount - 1, "$endIdx"] }
            }
          },

          // Expand to all bins in [startIdx..endIdx]
          {
            $project: {
              site_id: 1,
              binIdx: { $range: ["$startIdx", { $add: ["$endIdx", 1] }] }
            }
          },
          { $unwind: "$binIdx" },

          // Unique sites per bin: (binIdx, site_id)
          { $group: { _id: { binIdx: "$binIdx", site_id: "$site_id" } } },
          { $group: { _id: "$_id.binIdx", count: { $sum: 1 } } },
          { $sort: { _id: 1 } }
        ])
        .toArray();

      // ---- 3) sites_with_datings: sites that have ANY valid age or age_younger/age_older ----
      const [{ sites_with_datings = 0 } = {}] = await sitesCol
        .aggregate([
          { $match: { site_id: { $in: ids } } },
          { $unwind: "$analysis_entity_ages" },
          { $project: deriveYOProject },
          { $match: { y_raw: { $ne: null }, o_raw: { $ne: null } } },
          { $group: { _id: "$site_id" } },
          { $count: "sites_with_datings" }
        ])
        .toArray();

      // ---- 4) Materialize fixed 20-bin histogram ----
      const histogram = Array.from({ length: binCount }, (_, i) => ({
        bin_start: minAge + i * binSize,
        bin_end: minAge + (i + 1) * binSize,
        count: 0
      }));

      for (const row of counts) {
        const idx = row._id;
        if (Number.isInteger(idx) && idx >= 0 && idx < binCount) {
          histogram[idx].count = row.count;
        }
      }

      const resultObject = {
        cache_id: cacheKey,
        histogram,
        total_sites,
        sites_with_datings
      };

      if (this.app.useGraphCaching) {
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