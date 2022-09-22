const fs = require('fs');
const crypto = require('crypto');
const DendroLib = require('../DendroLib.class');

class DendrochronologyModule {
    constructor(app) {
        this.name = "Dendrochronology";
        this.moduleMethods = [10];
        this.app = app;
        this.expressApp = this.app.expressApp;
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
                console.error("Couldn't connect to database");
                console.error(error);
                return res.send(JSON.stringify({
                    error: "Internal server error"
                }));
            }
        });

        this.expressApp.post('/dendro/dating-histogram', async (req, res) => {
            let data = false;
            let cacheKeyString = this.app.appVersion+req.path+req.body.sites.join("");
            if(this.app.useQueryCaching) {
                data = this.readCache(cacheKeyString);
                console.log(req.path+" - cache hit");
            }
            if(!data) {
                data = await this.getDatingHistogramForSites(req.body.sites);
                this.writeCache(cacheKeyString, data);
                console.log(req.path+" - cache miss");
            }

            res.send({
                requestId: req.body.requestId,
                categories: data
            });
            
        });

        this.expressApp.post('/dendro/treespecies', async (req, res) => {
            let data = false;
            let cacheKeyString = this.app.appVersion+req.path+req.body.sites.join("");
            if(this.app.useQueryCaching) {
                data = this.readCache(cacheKeyString);
                console.log(req.path+" - cache hit");
            }
            if(!data) {
                data = await this.getTreeSpeciesForSites(req.body.sites);
                this.writeCache(cacheKeyString, data);
                console.log(req.path+" - cache miss");
            }

            res.send({
                requestId: req.body.requestId,
                categories: data
            });
        });

        this.expressApp.post('/dendro/featuretypes', async (req, res) => {
            let data = false;
            let cacheKeyString = this.app.appVersion+req.path+req.body.sites.join("");
            if(this.app.useQueryCaching) {
                data = this.readCache(cacheKeyString);
                console.log(req.path+" - cache hit");
            }
            if(!data) {
                data = await this.getFeatureTypesForSites(req.body.sites);
                this.writeCache(cacheKeyString, data);
                console.log(req.path+" - cache miss");
            }

            res.send({
                requestId: req.body.requestId,
                categories: data
            });
        });
    }

    siteHasModuleMethods(site) {
        for(let key in site.lookup_tables.analysis_methods) {
            if(this.moduleMethods.includes(site.lookup_tables.analysis_methods[key].method_id)) {
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
        let data = await pgClient.query("SELECT dendro_lookup_id, name, description FROM tbl_dendro_lookup");
        this.app.releaseDbConnection(pgClient);
        return data.rows;
    }

    async fetchSiteData(site) {
        if(!this.siteHasModuleMethods(site)) {
            return site;
        }
        let measurements = await this.getMeasurementsForSite(site.site_id);
        if(!site.data_groups) {
            site.data_groups = [];
        }
        site.data_groups = site.data_groups.concat(measurements);
        site.lookup_tables.dendro = await this.fetchDendroLookup();
        if(typeof site.lookup_tables.dating_uncertainty == "undefined") {
            site.lookup_tables.dating_uncertainty = [];
        }
        site.lookup_tables.dating_uncertainty = await this.fetchDatingUncertainty(site);
        site.lookup_tables.error_uncertainty = await this.fetchErrorUncertainty(site);

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
        SELECT ps.physical_sample_id,
        ae.analysis_entity_id,
        dl.name AS date_type,
        ps.sample_name AS sample,
        ps.date_sampled,
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
        WHERE tbl_sites.site_id=$1
        `;
        //SELECT * FROM postgrest_api.qse_dendro_measurements where site_id=2001 order by sample
        //let data = await pgClient.query('SELECT * FROM postgrest_api.qse_dendro_measurements WHERE site_id=$1', [siteId]);
        let data = await pgClient.query(sql, [siteId]);
        let measurementRows = data.rows;
        site.measurements = data.rows;

        sql = `
        SELECT DISTINCT ps.physical_sample_id,
        ae.analysis_entity_id,
        dl.name AS date_type,
        ps.sample_name AS sample,
        at.age_type,
        dd.age_older AS older,
        dd.age_younger AS younger,
        dd.error_plus AS plus,
        dd.error_minus AS minus,
        dd.dating_uncertainty_id,
        ddn.note AS dating_note,
        eu.error_uncertainty_type AS error_uncertainty,
        soq.season_or_qualifier_type AS season,
        dl.dendro_lookup_id,
        dl.description AS dendro_lookup_description,
        tbl_sites.site_id
        FROM tbl_physical_samples ps
        JOIN tbl_analysis_entities ae ON ps.physical_sample_id = ae.physical_sample_id
        JOIN tbl_dendro_dates dd ON ae.analysis_entity_id = dd.analysis_entity_id
        LEFT JOIN tbl_season_or_qualifier soq ON soq.season_or_qualifier_id = dd.season_or_qualifier_id
        LEFT JOIN tbl_age_types at ON at.age_type_id = dd.age_type_id
        LEFT JOIN tbl_error_uncertainties eu ON eu.error_uncertainty_id = dd.error_uncertainty_id
        LEFT JOIN tbl_dendro_lookup dl ON dd.dendro_lookup_id = dl.dendro_lookup_id
        LEFT JOIN tbl_sample_groups sg ON sg.sample_group_id = ps.sample_group_id
        LEFT JOIN tbl_dendro_date_notes ddn ON ddn.dendro_date_id = dd.dendro_date_id
        LEFT JOIN tbl_sites ON tbl_sites.site_id = sg.site_id
        WHERE tbl_sites.site_id=$1
        `;
        data = await pgClient.query(sql, [siteId]);
        let datingRows = data.rows;
        site.dating = data.rows;

        /* DISABLED THIS - not because it doesn't work but because I'm not sure where to put this data
        sql = `
        SELECT
        tbl_sites.site_id,
        tbl_physical_sample_features.*,
        tbl_features.*,
        tbl_feature_types.*,
        tbl_physical_samples.sample_group_id
        FROM
        tbl_sites
        LEFT JOIN tbl_sample_groups ON tbl_sample_groups.site_id = tbl_sites.site_id
        LEFT JOIN tbl_physical_samples ON tbl_physical_samples.sample_group_id=tbl_sample_groups.sample_group_id
        LEFT JOIN tbl_physical_sample_features ON tbl_physical_sample_features.physical_sample_id=tbl_physical_samples.physical_sample_id
        LEFT JOIN tbl_features ON tbl_features.feature_id=tbl_physical_sample_features.feature_id
        LEFT JOIN tbl_feature_types ON tbl_feature_types.feature_type_id=tbl_features.feature_type_id
        WHERE tbl_sites.site_id=$1
        `;

        data = await pgClient.query(sql, [siteId]);
        let sampleFeatures = data.rows;
        */

        this.app.releaseDbConnection(pgClient);

        let sampleDataObjects = this.dl.dbRowsToSampleDataObjects(measurementRows, datingRows);

        return sampleDataObjects;
    }

    async fetchDatingNotes(site) {

    }

    async fetchDatingUncertainty(site) {
        //Grab data in tbl_dating_uncertainty by dating_uncertainty_id
        let datingUncertaintyIds = [];
        site.data_groups.forEach(dg => {
            dg.datasets.forEach(dataset => {
                if(dataset.id == 134 || dataset.id == 137) {
                    if(parseInt(dataset.value.dating_uncertainty)) {
                        datingUncertaintyIds.push(dataset.value.dating_uncertainty);
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

    async fetchErrorUncertainty(site) {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }
        let sql = `SELECT * FROM tbl_error_uncertainties`;
        let errorUncertaintyData = await pgClient.query(sql);
        this.app.releaseDbConnection(pgClient);
        return errorUncertaintyData.rows;
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
        at.age_type,
        dd.age_older AS older,
        dd.age_younger AS younger,
        dd.error_plus AS plus,
        dd.error_minus AS minus,
        eu.error_uncertainty_type AS error_uncertainty,
        soq.season_or_qualifier_type AS season,
        dl.dendro_lookup_id,
        dl.description AS dendro_lookup_description,
        tbl_sites.site_id
        FROM tbl_physical_samples ps
        JOIN tbl_analysis_entities ae ON ps.physical_sample_id = ae.physical_sample_id
        JOIN tbl_dendro_dates dd ON ae.analysis_entity_id = dd.analysis_entity_id
        LEFT JOIN tbl_season_or_qualifier soq ON soq.season_or_qualifier_id = dd.season_or_qualifier_id
        LEFT JOIN tbl_age_types at ON at.age_type_id = dd.age_type_id
        LEFT JOIN tbl_error_uncertainties eu ON eu.error_uncertainty_id = dd.error_uncertainty_id
        LEFT JOIN tbl_dendro_lookup dl ON dd.dendro_lookup_id = dl.dendro_lookup_id
        LEFT JOIN tbl_sample_groups sg ON sg.sample_group_id = ps.sample_group_id
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

        return featureTypeCategories;
    }

    async getTreeSpeciesForSites(siteIds) {
        if(!siteIds) {
            siteIds = [];
        }
        let sites = [];

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

        let treeSpeciesCategories = [];

        sites.forEach(site => {
            site.sampleDataObjects.forEach(sampleDataObject => {
                let species = this.dl.getDendroMeasurementByName("Tree species", sampleDataObject);
                if(!species) {
                    return false;
                }

                species = species.toLowerCase();
    
                let foundSpecies = false;
                treeSpeciesCategories.forEach(speciesCat => {
                    if(speciesCat.name == species) {
                        speciesCat.count++;
                        foundSpecies = true;
                    }
                });
    
                if(!foundSpecies) {
                    treeSpeciesCategories.push({
                        name: species,
                        count: 1
                    });
                }
            });
        })

        return treeSpeciesCategories;
    }

    async getDatingHistogramForSites(siteIds) {
        if(!siteIds) {
            siteIds = [];
        }
        let sites = [];

        if(siteIds.length == 0) {
            //If no sites are selected we assume ALL sites
            console.log("Getting ALL sites")
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

                    cat.datingsNum += selectedSampleDataObjectsHighReliability.length;
                    cat.datingsNumUncertain += selectedSampleDataObjects.length;
                }
                
                
            });
        });

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

    postProcessSiteData(site) {
        return site;
    }

}

module.exports = DendrochronologyModule;