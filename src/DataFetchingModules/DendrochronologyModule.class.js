import fs from 'fs';
import crypto from 'crypto';
import DendroLib from '../Lib/sead_common/DendroLib.class.js';


class DendrochronologyModule {
    constructor(app) {
        this.name = "Dendrochronology";
        this.moduleMethods = [10];
        this.app = app;
        this.expressApp = this.app.expressApp;
        //this.ringWidthsTestData = fs.readFileSync("0RTM1739.rwl.json");
        //this.ringWidthsTestData = JSON.parse(this.ringWidthsTestData.toString());
        this.dl = new DendroLib();
        this.setupEndpoints();
    }

    setupEndpoints() {

        this.expressApp.get('/dendro-lookup', async (req, res) => {
            try {
                const pgClient = await this.app.getDbConnection();
                await pgClient.query('SELECT * FROM tbl_dendro_lookup').then((data) => {
                    this.app.releaseDbConnection(pgClient);
                    return res.send(data);
                });
            }
            catch(error) {
                console.error("Couldn't connect to postgres database");
                console.error(error);
                return res.send(JSON.stringify({
                    error: "Internal server error"
                }));
            }
        });

        this.expressApp.post('/dendro/dating-histogram', async (req, res) => {
            let data = await this.getDatingHistogramForSites(req.body.sites);
            res.send({
                requestId: req.body.requestId,
                categories: data
            });
        });

        this.expressApp.post('/dendro/treespecies', async (req, res) => {
            let data = await this.getTreeSpeciesForSites(req.body.sites);
            res.send({
                requestId: req.body.requestId,
                categories: data
            });
        });

        this.expressApp.post('/dendro/dynamicchart', async (req, res) => {
            let data = await this.getDendroVariableForSites(req.body.sites, req.body.variable);
            res.send({
                requestId: req.body.requestId,
                categories: data
            });
        });

        this.expressApp.post('/dendro/featuretypes', async (req, res) => {
            let data = await this.getFeatureTypesForSites(req.body.sites);
            res.send({
                requestId: req.body.requestId,
                categories: data
            });
        });
    }

    siteHasModuleMethods(site) {
        for(let key in site.lookup_tables.methods) {
            if(this.moduleMethods.includes(site.lookup_tables.methods[key].method_id)) {
                return true;
            }
        }
        return false;
    }

    async fetchDendroLookup() {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }
        let valueClasses = await pgClient.query(`
            SELECT DISTINCT(tbl_value_classes.value_class_id)
            FROM tbl_analysis_values
            LEFT JOIN tbl_value_classes ON tbl_value_classes.value_class_id=tbl_analysis_values.value_class_id
            WHERE tbl_value_classes.method_id=10
            `);

        let valueClassIds = valueClasses.rows.map(row => row.value_class_id);

        let data = await pgClient.query(`
            SELECT DISTINCT ON (tbl_analysis_values.value_class_id)
                tbl_analysis_values.value_class_id,
                tbl_value_classes.value_type_id,
                tbl_value_classes.method_id,
                tbl_value_classes.parent_id,
                tbl_value_classes.name,
                tbl_value_classes.description
            FROM tbl_analysis_values
            LEFT JOIN tbl_value_classes 
                ON tbl_value_classes.value_class_id = tbl_analysis_values.value_class_id
            WHERE tbl_value_classes.value_class_id = ANY($1::int[])
            ORDER BY tbl_analysis_values.value_class_id, tbl_value_classes.name;
            `, [valueClassIds]);

        this.app.releaseDbConnection(pgClient);
        return data.rows;
    }

    async fetchSiteData(site, verbose = false) {
        if(!this.siteHasModuleMethods(site)) {
            return site;
        }

        if(verbose) {
            console.log("Fetching dendrochronology data for site "+site.site_id);
        }

        const ringWidths = await this.fetchRingWidthsForSite(site.site_id);
        let measurements = await this.getMeasurementsForSite(site.site_id);

        let dataGroups = [];
        let dataGroupId = 1;
        measurements.forEach(measurement => {
            /*
            measurement = sampleDataObject = {
                id: physicalSampleId,
                method_id: 10,
                type: "dendro",
                sample_name: "",
                date_sampled: "",
                physical_sample_id: physicalSampleId,
                datasets: []
            }
            */

            //put all the measurements with the same physical_sample_id in the same data group
            let dataGroup = dataGroups.find(dg => dg.physical_sample_id == measurement.physical_sample_id);


            //get all biblio_ids from the datasets if they exist
            let datasetBiblioIds = measurement.datasets.map(ds => {
                if(ds.biblio_id) {
                    return ds.biblio_id;
                }
                return null;
            });
            datasetBiblioIds = datasetBiblioIds.filter(biblioId => biblioId != null);
            //make them unique
            datasetBiblioIds = datasetBiblioIds.filter((value, index, self) => {
                return self.indexOf(value) === index;
            });

            if(!dataGroup) {
                dataGroup = {
                    data_group_id: dataGroupId++,
                    physical_sample_id: measurement.physical_sample_id,
                    sample_name: measurement.sample_name,
                    date_sampled: measurement.date_sampled,
                    biblio_ids: datasetBiblioIds ? datasetBiblioIds : [],
                    method_ids: [10],
                    method_group_ids: [],
                    values: []
                };

                dataGroups.push(dataGroup);
            }
            else {
                //only concat if unique
                dataGroup.biblio_ids = dataGroup.biblio_ids.concat(measurement.biblio_ids.filter(biblioId => !dataGroup.biblio_ids.includes(biblioId)));
            }

            measurement.datasets.forEach(ds => {
                dataGroup.values.push({
                    analysis_entitity_id: null,
                    dataset_id: null,
                    valueClassId: ds.id,
                    key: ds.label, 
                    value: ds.value,
                    valueType: ds.valueType,
                    data: ds.complexValue,
                    methodId: 10,
                });
            });

            if(ringWidths) {
                dataGroup.values.push({
                    analysis_entitity_id: null,
                    dataset_id: null,
                    valueClassId: null,
                    key: "Ring widths", 
                    value: [],
                    valueType: 'complex',
                    data: ringWidths,
                    methodId: 10,
                });
            }
        });

        if(!site.data_groups) {
            site.data_groups = [];
        }
        //site.data_groups = site.data_groups.concat(measurements);
        //site.data_groups = dataGroups;

        site.data_groups = site.data_groups.concat(dataGroups);

        site.lookup_tables.dendro = await this.fetchDendroLookup();
        if(typeof site.lookup_tables.dating_uncertainty == "undefined") {
            site.lookup_tables.dating_uncertainty = [];
        }
        site.lookup_tables.dating_uncertainty = await this.fetchDatingUncertainty(site);

        return site;
    }

    async fetchDendroSite(siteId, mergeWithSite = null) {

    }

    getDendroSite(siteId, mergeWithSite = null) {
    }

    async getMeasurementsForSite(siteId) {
        let site = {
            siteId: siteId
        };
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = `
        SELECT 
        tbl_analysis_values.*,
        tbl_value_classes.*,
        tbl_physical_samples.physical_sample_id,
        tbl_physical_samples.sample_name,
		tbl_physical_samples.date_sampled,
		tbl_analysis_dating_ranges.low_value AS dating_range_low_value,
		tbl_analysis_dating_ranges.high_value AS dating_range_high_value,
		tbl_analysis_dating_ranges.low_is_uncertain AS dating_range_low_is_uncertain,
		tbl_analysis_dating_ranges.high_is_uncertain AS dating_range_high_is_uncertain,
		tbl_analysis_dating_ranges.low_qualifier AS dating_range_low_qualifier,
		tbl_analysis_dating_ranges.age_type_id AS dating_range_age_type_id,
		tbl_analysis_dating_ranges.season_id AS dating_range_season_id,
		tbl_analysis_dating_ranges.dating_uncertainty_id AS dating_range_dating_uncertainty_id,
		tbl_analysis_dating_ranges.is_variant AS dating_range_is_variant,
		tbl_datasets.dataset_id,
		tbl_datasets.biblio_id
        FROM
        tbl_analysis_values
        LEFT JOIN tbl_value_classes ON tbl_value_classes.value_class_id=tbl_analysis_values.value_class_id
        JOIN tbl_analysis_entities ON tbl_analysis_entities.analysis_entity_id=tbl_analysis_values.analysis_entity_id
        JOIN tbl_physical_samples ON tbl_physical_samples.physical_sample_id=tbl_analysis_entities.physical_sample_id
        LEFT JOIN tbl_sample_groups sg ON sg.sample_group_id = tbl_physical_samples.sample_group_id
        LEFT JOIN tbl_sites ON tbl_sites.site_id = sg.site_id
		LEFT JOIN tbl_analysis_dating_ranges ON tbl_analysis_dating_ranges.analysis_value_id=tbl_analysis_values.analysis_value_id
		LEFT JOIN tbl_datasets ON tbl_datasets.dataset_id=tbl_analysis_entities.dataset_id
        WHERE tbl_sites.site_id=$1
        `;

        //tbl_analysis_dating_ranges

        let data = await pgClient.query(sql, [siteId]);
        site.measurements = data.rows;

        this.app.releaseDbConnection(pgClient);
        let sampleDataObjects = this.dl.dbRowsToSampleDataObjects(site.measurements);
        return sampleDataObjects;
    }

    async fetchRingWidthsForSite(siteId) {
        return null;
    }

    async fetchDatingNotes(site) {

    }

    async fetchDatingUncertainty(site) {
        //Grab data in tbl_dating_uncertainty by dating_uncertainty_id
        let datingUncertaintyIds = [];
        site.data_groups.forEach(dg => {
            dg.values.forEach(dataset => {
                if(dataset.id == 134 || dataset.id == 137) {
                    if(parseInt(dataset.data.dating_uncertainty)) {
                        datingUncertaintyIds.push(dataset.data.dating_uncertainty);
                    }
                }
            });
        });

        //Make them unique
        datingUncertaintyIds = datingUncertaintyIds.filter((value, index, self) => {
            return self.indexOf(value) === index;
        });

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let datingUncertaintyLookupTable = [];
        let sql = `SELECT * FROM tbl_dating_uncertainty WHERE dating_uncertainty_id=$1`;
        for(let key in datingUncertaintyIds) {
            let datingUncertaintyRows = await pgClient.query(sql, [datingUncertaintyIds[key]]);
            datingUncertaintyLookupTable.push(datingUncertaintyRows.rows[0]);
        }
        this.app.releaseDbConnection(pgClient);

        return datingUncertaintyLookupTable;
    }

    async getMeasurementsForAllSites() {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = `
        SELECT ps.physical_sample_id,
        ae.analysis_entity_id,
        dl.name AS date_type,
        ps.sample_name AS sample,
        dl.dendro_lookup_id,
        dl.description AS dendro_lookup_description,
        tbl_sites.site_id,
        tbl_dendro.measurement_value
        FROM tbl_physical_samples ps
        JOIN tbl_analysis_entities ae ON ps.physical_sample_id = ae.physical_sample_id
        JOIN tbl_dendro ON tbl_dendro.analysis_entity_id = ae.analysis_entity_id
        LEFT JOIN tbl_dendro_lookup dl ON tbl_dendro.dendro_lookup_id = dl.dendro_lookup_id
        LEFT JOIN tbl_sample_groups sg ON sg.sample_group_id = ps.sample_group_id
        LEFT JOIN tbl_sites ON tbl_sites.site_id = sg.site_id
        `;
        //let data = await pgClient.query('SELECT * FROM postgrest_api.qse_dendro_measurements');
        let data = await pgClient.query(sql);
        let measurementRows = data.rows;

        sql = `
        SELECT DISTINCT ps.physical_sample_id,
        ae.analysis_entity_id,
        dl.name AS date_type,
        ps.sample_name AS sample,
        agetype.age_type,
        dd.age_older AS older,
        dd.age_younger AS younger,
        dd.dating_uncertainty_id,
        dd.season_id,
        tbl_seasons.season_type_id,
        tbl_seasons.season_name,
        ddn.note AS dating_note,
        dl.dendro_lookup_id,
        dl.description AS dendro_lookup_description,
        tbl_sites.site_id,
        tbl_datasets.dataset_id,
		tbl_datasets.biblio_id
        FROM tbl_physical_samples ps
        JOIN tbl_analysis_entities ae ON ps.physical_sample_id = ae.physical_sample_id
        JOIN tbl_datasets ON tbl_datasets.dataset_id=ae.dataset_id
        JOIN tbl_dendro_dates dd ON ae.analysis_entity_id = dd.analysis_entity_id
        LEFT JOIN tbl_seasons ON tbl_seasons.season_id = dd.season_id
        LEFT JOIN tbl_age_types agetype ON agetype.age_type_id = dd.age_type_id
        LEFT JOIN tbl_dendro_lookup dl ON dd.dendro_lookup_id = dl.dendro_lookup_id
        LEFT JOIN tbl_sample_groups sg ON sg.sample_group_id = ps.sample_group_id
        LEFT JOIN tbl_dendro_date_notes ddn ON ddn.dendro_date_id = dd.dendro_date_id
        LEFT JOIN tbl_sites ON tbl_sites.site_id = sg.site_id
        `;

        //data = await pgClient.query('SELECT * FROM postgrest_api.qse_dendro_dating2');
        data = await pgClient.query(sql);
        let datingRows = data.rows;
        this.app.releaseDbConnection(pgClient);

        let siteIds = [];
        let sites = [];

        //Sort data rows into their respective sites
        for(let key in measurementRows) {
            siteIds.push(measurementRows[key].site_id);
        }
        for(let key in datingRows) {
            siteIds.push(datingRows[key].site_id);
        }

        siteIds = siteIds.filter((value, index, self) => {
            return self.indexOf(value) === index;
        });

        for(let key in siteIds) {
            let selectedMeasurementRows = this.getAllMeasurementRowsForSite(siteIds[key], measurementRows);
            let selectedDatingRows = this.getAllDatingRowsForSite(siteIds[key], datingRows);
            let sampleDataObjects = this.dl.dbRowsToSampleDataObjects(selectedMeasurementRows, selectedDatingRows);
            sites.push({
                siteId: siteIds[key],
                sampleDataObjects: sampleDataObjects
            });
        }
        return sites;
    }

    getAllMeasurementRowsForSite(siteId, measurementRows) {
        let selectedRows = [];
        for(let key in measurementRows) {
            if(measurementRows[key].site_id == siteId) {
                selectedRows.push(measurementRows[key])
            }
        }
        return selectedRows;
    }

    getAllDatingRowsForSite(siteId, datingRows) {
        let selectedRows = [];
        for(let key in datingRows) {
            if(datingRows[key].site_id == siteId) {
                selectedRows.push(datingRows[key])
            }
        }
        return selectedRows;
    }

    getDatingHistogramFromCache(siteIds) {
        let sha = crypto.createHash('sha1');
        let hashKey = siteIds.toString();
        if(siteIds.length == 0) {
            hashKey = "all sites";
        }
        sha.update(hashKey);
        let hash = sha.digest('hex');
        try {
            let data = fs.readFileSync("query_cache/"+hash+".json", {
                encoding: 'utf8'
            });
            if(data) {
                console.log("Cache result found for this query.");
                return JSON.parse(data);
            }
        }
        catch(error) {
            //Nope, no cache for this query
            console.log("No cache result for this query.");
            return false;
        }
    }

    async getFeatureTypesForSites(siteIds) {
        if(!siteIds) {
            siteIds = [];
        }
        let sites = [];

        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('getFeatureTypesForSites'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) {
          return cachedData.data;
        }

        if(siteIds.length == 0) {
            //If no sites are selected we assume ALL sites
            sites = await this.getMeasurementsForAllSites();
        }
        else {
            for(let key in siteIds) {
                let sampleDataObjects = await this.getMeasurementsForSite(siteIds[key]);
                sites.push({
                    siteId: siteIds[key],
                    sampleDataObjects: sampleDataObjects
                });
            }
        }

        let featureTypeCategories = [];

        sites.forEach(site => {
            site.sampleDataObjects.forEach(sampleDataObject => {
                //let species = this.dl.getDendroMeasurementByName("Tree species", sampleDataObject);
                console.log(sampleDataObject);
            });
        })

        let resultObject = {
            cache_id: cacheId,
            data: featureTypeCategories
        };
        
        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);

        return featureTypeCategories;
    }


    async getDendroVariableForSites(siteIds, dendroVariable = "Tree species") {
        if(!siteIds) {
            siteIds = [];
        }

        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('getTreeSpeciesForSites'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) {
          return cachedData.data;
        }

        if(siteIds.length == 0) {
            let pgClient = await this.app.getDbConnection();
            if(!pgClient) {
                return false;
            }

            let sql = `SELECT site_id FROM tbl_sites;`;
            //let data = await pgClient.query('SELECT * FROM postgrest_api.qse_dendro_measurements');
            let data = await pgClient.query(sql);
            data.rows.forEach(row => {
                siteIds.push(row.site_id);
            });
            this.app.releaseDbConnection(pgClient);
        }
        
        let data = this.app.mongo.collection("sites").aggregate([
            // Match documents that contain the specified site IDs
            {
                $match: {
                    'data_groups.values.key': dendroVariable,
                    'site_id': { $in: siteIds },
                }
            },
            // Unwind the arrays
            { $unwind: '$data_groups' },
            { $unwind: '$data_groups.values' },
            // Filter only the selected key
            {
              $match: {
                'data_groups.values.key': dendroVariable
              }
            },
            // Group by selected variable and count occurrences
            {
              $group: {
                _id: '$data_groups.values.value',
                count: { $sum: 1 }
              }
            },
            // Project to reshape the output
            {
              $project: {
                _id: 0,
                name: '$_id',
                count: 1
              }
            }
          ]);

          let treeSpeciesCategories = await data.toArray();          
          return treeSpeciesCategories;
    }

    async getTreeSpeciesForSites(siteIds) {
        if(!siteIds) {
            siteIds = [];
        }

        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('getTreeSpeciesForSites'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) {
          return cachedData.data;
        }

        if(siteIds.length == 0) {
            let pgClient = await this.app.getDbConnection();
            if(!pgClient) {
                return false;
            }

            let sql = `SELECT site_id FROM tbl_sites;`;
            //let data = await pgClient.query('SELECT * FROM postgrest_api.qse_dendro_measurements');
            let data = await pgClient.query(sql);
            data.rows.forEach(row => {
                siteIds.push(row.site_id);
            });
            this.app.releaseDbConnection(pgClient);
        }
        
        let data = this.app.mongo.collection("sites").aggregate([
            // Match documents that contain the specified site IDs
            {
                $match: {
                    'data_groups.values.key': 'Tree species',
                    'site_id': { $in: siteIds },
                }
            },
            // Unwind the arrays
            { $unwind: '$data_groups' },
            { $unwind: '$data_groups.values' },
            // Filter only the 'Tree species' key
            {
              $match: {
                'data_groups.values.key': 'Tree species'
              }
            },
            // Group by tree species and count occurrences
            {
              $group: {
                _id: '$data_groups.values.value',
                count: { $sum: 1 }
              }
            },
            // Project to reshape the output
            {
              $project: {
                _id: 0,
                name: '$_id',
                count: 1
              }
            }
          ]);

          let treeSpeciesCategories = await data.toArray();          
          return treeSpeciesCategories;
    }

    async getDatingHistogramForSitesPIPE(siteIds) {
        if (!siteIds) {
            siteIds = [];
        }
    
        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('datinghistogramforsites' + JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
    
        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) {
            return cachedData.data;
        }
    
        let matchStage = {};
        if (siteIds.length > 0) {
            matchStage = { siteId: { $in: siteIds } };
        }
    
        const aggregationPipeline = [
            { $match: matchStage },
            {
                $unwind: "$sampleDataObjects"
            },
            {
                $project: {
                    siteId: 1,
                    germinationYearOldest: "$sampleDataObjects.germinationYearOldest",
                    fellingYearYoungest: "$sampleDataObjects.fellingYearYoungest",
                    reliability: "$sampleDataObjects.reliability"
                }
            },
            {
                $group: {
                    _id: null,
                    oldestYear: { $min: "$germinationYearOldest" },
                    youngestYear: { $max: "$fellingYearYoungest" }
                }
            }
        ];

        let collection = this.app.mongo.collection("sites");
    
        let [yearRange] = await collection.aggregate(aggregationPipeline).toArray();
        if (!yearRange || !yearRange.oldestYear || !yearRange.youngestYear) {
            return [];
        }
    
        let catSize = 20;
        let categoriesNum = Math.ceil((yearRange.youngestYear - yearRange.oldestYear) / catSize);
        let categories = [];
        let prevCatEnd = 0;
        for (let i = 0; i < categoriesNum; i++) {
            categories.push({
                startYear: yearRange.oldestYear + prevCatEnd,
                endYear: yearRange.oldestYear + (catSize * (i + 1))
            });
            prevCatEnd = catSize * (i + 1);
        }
        categories[categories.length - 1].endYear = yearRange.youngestYear;
    
        let categoryStages = categories.map((cat, index) => ({
            $facet: {
                [`cat${index}`]: [
                    { $match: { "sampleDataObjects.germinationYearOldest": { $gte: cat.startYear, $lt: cat.endYear } } },
                    { $group: { _id: null, count: { $sum: 1 } } }
                ]
            }
        }));
    
        let sampleCounts = await collection.aggregate(categoryStages).toArray();
    
        categories.forEach((cat, index) => {
            cat.datingsNum = sampleCounts[`cat${index}`][0] ? sampleCounts[`cat${index}`][0].count : 0;
        });
    
        let resultObject = {
            cache_id: cacheId,
            data: categories
        };
    
        await this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
    
        return categories;
    }
    

    async getDatingHistogramForSites(siteIds) {
        if(!siteIds) {
            siteIds = [];
        }
        let sites = [];

        let cacheId = crypto.createHash('sha256');
        siteIds.sort((a, b) => a - b);
        cacheId = cacheId.update('datinghistogramforsites'+JSON.stringify(siteIds)).digest('hex');
        let identifierObject = { cache_id: cacheId };
      
        let cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
        if (cachedData !== false) {
            return cachedData.data;
        }

        if(siteIds.length == 0) {
            //If no sites are selected we assume ALL sites
            console.log("Getting ALL sites")
            sites = await this.getMeasurementsForAllSites();
        }
        else {
            console.log("Getting "+siteIds.length+" sites");
            for(let key in siteIds) {
                let sampleDataObjects = await this.getMeasurementsForSite(siteIds[key]);
                sites.push({
                    siteId: siteIds[key],
                    sampleDataObjects: sampleDataObjects
                });
            }
        }
        
        //1. Find out the total year span for datings within these sites
        let oldestYear = null;
        let youngestYear = null;
        sites.forEach(site => {
            site.sampleDataObjects.forEach(dataObject => {
                let germinationYearOldest = this.dl.getOldestGerminationYear(dataObject);
                if(germinationYearOldest.value != null && (oldestYear == null || germinationYearOldest.value < oldestYear.value)) {
                    oldestYear = germinationYearOldest;
                }
            })
        });
        
        sites.forEach(site => {
            site.sampleDataObjects.forEach(dataObject => {
                let fellingYearYoungest = this.dl.getYoungestFellingYear(dataObject);
                if(fellingYearYoungest.value != null && (youngestYear == null || fellingYearYoungest.value > youngestYear.value)) {
                    youngestYear = fellingYearYoungest;
                }
            })
        });

        //If there's not enough data to set a youngest or oldest year, we can't continue
        if(oldestYear == null || youngestYear == null) {
            return [];
        }

        //2. Divide year span into categories/bars
        //const categoriesNum = 20;
        //778 - 1883
        let catSize = 20;
        let categoriesNum = Math.ceil((youngestYear.value - oldestYear.value) / catSize);
        let categories = [];
        let prevCatEnd = 0;
        for(let i = 0; i < categoriesNum; i++) {
            categories.push({
                startYear: oldestYear.value + prevCatEnd,
                endYear: oldestYear.value + (catSize * (i+1))
            });
            prevCatEnd = catSize * (i+1);
        }

        categories.forEach(cat => {
            cat.startTs = new Date(cat.startYear.toString()).getTime();
            cat.endTs = new Date(cat.endYear.toString()).getTime();
        });

        //Here we get to choose which way this graph will be misleading
        //Will it be misleading the way that it does not show the full total year span,
        //or in the way that the last bar has a slightly longer span? I choose the latter
        categories[categories.length-1].endYear = youngestYear.value;

        //3. Find out number of dated samples which fall within each category/bar
        categories.forEach(cat => {
            cat.datingsNum = 0;
            cat.datingsNumHighReliability = 0;
            sites.forEach(site => {

                let selectedSampleDataObjects = this.dl.getSamplesWithinTimespan(site.sampleDataObjects, cat.startYear, cat.endYear);

                if(selectedSampleDataObjects) {

                    /*
                    //Further divide the selected samples in reliability categories
                    let selectedSampleDataObjectsHighReliability = selectedSampleDataObjects.filter(sampleDataObject => {
                        let oldestGerminationYear = this.dl.getOldestGerminationYear(sampleDataObject);
                        let youngestFellingYear = this.dl.getYoungestFellingYear(sampleDataObject);

                        if(!oldestGerminationYear.value || !youngestFellingYear.value) {
                            return false;
                        }
                        
                        if(oldestGerminationYear.reliability == 1 && youngestFellingYear.reliability == 1) {
                            return true;
                        }
                        return false;
                    });
                    */

                    cat.datingsNum += selectedSampleDataObjects.length;
                    cat.datingsNumUncertain += selectedSampleDataObjects.length;
                }
            });
        });

        let resultObject = {
            cache_id: cacheId,
            data: categories
        };
        
        this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);

        return categories;
    }

    writeCache(keyString, data) {
        let sha = crypto.createHash('sha1');
        sha.update(keyString);
        let hash = sha.digest('hex');
        fs.writeFile("query_cache/"+hash+".json", JSON.stringify(data, null, 2), err => {
            if (err) {
                console.error(err);
                return;
            }
        });
    }

    readCache(keyString) {
        let sha = crypto.createHash('sha1');
        sha.update(keyString);
        let hash = sha.digest('hex');
        try {
            let data = fs.readFileSync("query_cache/"+hash+".json", {
                encoding: 'utf8'
            });
            if(data) {
                console.log("Cache result found for this query.");
                return JSON.parse(data);
            }
        }
        catch(error) {
            //Nope, no cache for this query
            console.log("No cache result for this query.");
            return false;
        }
    }

    readCacheMongo(keyString) {

    }

    postProcessSiteData(site) {
        return site;
    }

}

export default DendrochronologyModule;