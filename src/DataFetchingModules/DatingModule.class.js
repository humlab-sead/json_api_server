const fs = require('fs');

class DatingModule {
    constructor(app) {
        this.name = "Dating";
        this.moduleMethodGroups = [19, 20, 3]; //should maybe include 21 as well...
        this.c14StdMethodIds = [151, 148, 38, 150];
        this.app = app;
        this.expressApp = this.app.expressApp;
    }

    siteHasModuleMethods(site) {
        for(let key in site.lookup_tables.analysis_methods) {
            if(this.moduleMethodGroups.includes(site.lookup_tables.analysis_methods[key].method_group_id)) {
                return true;
            }
        }
        return false;
    }
    
    async fetchSiteData(site, verbose = false) {
        if(!this.siteHasModuleMethods(site)) {
            return site;
        }

        if(verbose) {
            console.log("Fetching dating data for site "+site.site_id);
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = `
        SELECT
        tbl_relative_dates.relative_date_id,
        tbl_relative_dates.relative_age_id,
        tbl_relative_dates.method_id,
        tbl_relative_dates.notes,
        tbl_relative_dates.dating_uncertainty_id,
        tbl_relative_ages.relative_age_id,
        tbl_relative_ages.relative_age_type_id,
        tbl_relative_ages.relative_age_name,
        tbl_relative_ages.description AS rel_age_desc,
        tbl_relative_ages.c14_age_older,
        tbl_relative_ages.c14_age_younger,
        tbl_relative_ages.cal_age_older,
        tbl_relative_ages.cal_age_younger,
        tbl_relative_ages.notes AS relative_age_notes,
        tbl_relative_ages.location_id AS relative_age_location_id,
        tbl_relative_ages.abbreviation AS relative_age_abbreviation,
        tbl_relative_age_types.relative_age_type_id,
        tbl_relative_age_types.age_type,
        tbl_relative_age_types.description as age_description,
        tbl_locations.location_name AS age_location_name,
        tbl_locations.location_type_id AS age_location_type_id,
        tbl_locations.default_lat_dd AS age_default_lat_dd,
        tbl_locations.default_long_dd AS age_default_long_dd,
        tbl_location_types.location_type AS age_location_type,
        tbl_location_types.description AS age_location_desc
        FROM public.tbl_relative_dates
        LEFT JOIN tbl_relative_ages ON tbl_relative_ages.relative_age_id = tbl_relative_dates.relative_age_id
        LEFT JOIN tbl_relative_age_types ON tbl_relative_age_types.relative_age_type_id = tbl_relative_ages.relative_age_type_id
        LEFT JOIN tbl_locations ON tbl_locations.location_id = tbl_relative_ages.location_id
        LEFT JOIN tbl_location_types ON tbl_location_types.location_type_id = tbl_locations.location_type_id
        WHERE analysis_entity_id=$1;
        `;

        let c14stdSql = `
        SELECT 
        tbl_analysis_entities.*,
        tbl_geochronology.*,
        tbl_dating_uncertainty.description AS dating_uncertainty_desc,
        tbl_dating_uncertainty.uncertainty AS dating_uncertainty
        FROM tbl_analysis_entities
        JOIN tbl_geochronology ON tbl_geochronology.analysis_entity_id = tbl_analysis_entities.analysis_entity_id
        LEFT JOIN tbl_dating_uncertainty ON tbl_dating_uncertainty.dating_uncertainty_id = tbl_geochronology.dating_uncertainty_id
        WHERE tbl_analysis_entities.analysis_entity_id=$1;
        `;
        
        let queriesExecuted = 0;
        let queryPromises = [];
        
        site.sample_groups.forEach(sampleGroup => {
            sampleGroup.physical_samples.forEach(physicalSample => {
                physicalSample.analysis_entities.forEach(analysisEntity => {

                    //Here we need to check what dataset this AE is linked to
                    //if it is a dataset with method_id 151 then it's 'C14 std'
                    //which needs to be handled/fetched differently from the other dating methods

                    let specialTreatment = false;
                    for(let key in site.datasets) {
                        if(analysisEntity.dataset_id == site.datasets[key].dataset_id) {
                            if(this.c14StdMethodIds.includes(site.datasets[key].method_id)) {
                                //This needs special treatment
                                specialTreatment = true;
                            }
                        }
                    }

                    let promise = null;
                    if(!specialTreatment) {
                        promise = pgClient.query(sql, [analysisEntity.analysis_entity_id]).then(values => {
                            analysisEntity.dating_values = values.rows[0];
                        });
                        queriesExecuted++;
                    }
                    else {
                        promise = pgClient.query(c14stdSql, [analysisEntity.analysis_entity_id]).then(values => {
                            let r = values.rows[0];
                            analysisEntity.dating_values = {
                                "geochron_id": r.geochron_id,
                                "dating_lab_id": r.dating_lab_id,
                                "lab_number": r.lab_number,
                                "age": r.age,
                                "error_older": r.error_older,
                                "error_younger": r.error_younger,
                                "delta_13c": r.delta_13c,
                                "notes": r.notes,
                                "dating_uncertainty_id": r.dating_uncertainty_id,
                                "dating_uncertainty": r.dating_uncertainty,
                                "dating_uncertainty_desc": r.dating_uncertainty_desc
                            };

                            //if dating_lab_id seems to be a number and we don't already have his lab 
                            //among the lookups, add it
                            const datingLabId = parseInt(r.dating_lab_id);
                            if(datingLabId != NaN) {
                                let labFetchPromise = this.fetchDatingLab(site, datingLabId);
                                queryPromises.push(labFetchPromise);
                            }
                        });
                        queriesExecuted++;
                    }
                    queryPromises.push(promise);
                    
                });
            })
        });

        await Promise.all(queryPromises).then(() => {
            this.app.releaseDbConnection(pgClient);
            //console.log("Dating module executed "+queriesExecuted+" queries for site "+site.site_id);
        });

        return site;
    }

    getNormalizedDatingSpanFromDataGroup(dataGroup) {
        let older = null;
        let younger = null;
        let type = "";
        if(this.c14StdMethodIds.includes(dataGroup.method_id)) {
            type = "c14std";
            dataGroup.data_points.forEach(dp => {
                older = parseInt(dp.dating_values.age) - parseInt(dp.dating_values.error_older);
                younger = parseInt(dp.dating_values.cal_age_younger) + parseInt(dp.dating_values.error_younger);
            });
        }
        else {
            type = "modern";
            dataGroup.data_points.forEach(dp => {
                older = parseInt(dp.dating_values.cal_age_older);
                younger = parseInt(dp.dating_values.cal_age_younger);
            });
        }

        return {
            type: type,
            older: older,
            younger: younger
        }
    }

    getNormalizedDatingSpanFromDataset(dataset) {
        let dating = {
            older: null,
            younger: null,
            dataset_id: dataset.dataset_id
        }

        for(let aeKey in dataset.analysis_entities) {
            let ae = dataset.analysis_entities[aeKey];
            
            if(ae.dating_values) {
                let datingValues = ae.dating_values;

                //c14
                if(datingValues.c14_age_older && datingValues.c14_age_younger) {
                    let olderValue = parseInt(datingValues.c14_age_older);
                    let youngerValue = parseInt(datingValues.c14_age_younger);                    

                    //dates are always BP so bigger is older
                    if(olderValue > dating.older || dating.older == null) {
                        dating.older = olderValue;
                        dating.older_analysis_entity_id = ae.analysis_entity_id;
                    }
                    if(youngerValue < dating.younger || dating.younger == null) {
                        dating.younger = youngerValue;
                        dating.younger_analysis_entity_id = ae.analysis_entity_id;
                    }
                }

                //cal
                if(datingValues.cal_age_older && datingValues.cal_age_younger) {
                    let olderValue = parseInt(datingValues.cal_age_older);
                    let youngerValue = parseInt(datingValues.cal_age_younger);

                    if(olderValue > dating.older || dating.older == null) {
                        dating.older = olderValue;
                        dating.older_analysis_entity_id = ae.analysis_entity_id;
                    }
                    if(youngerValue < dating.younger || dating.younger == null) {
                        dating.younger = youngerValue;
                        dating.younger_analysis_entity_id = ae.analysis_entity_id;
                    }
                }

                //other
                if(datingValues.age && datingValues.error_older && datingValues.error_younger) {
                    let olderValue = parseInt(datingValues.age) - parseInt(datingValues.error_older);
                    let youngerValue = parseInt(datingValues.age) + parseInt(datingValues.error_younger);


                    if(olderValue > dating.older || dating.older == null) {
                        dating.older = olderValue;
                        dating.older_analysis_entity_id = ae.analysis_entity_id;
                    }
                    if(youngerValue < dating.younger || dating.younger == null) {
                        dating.younger = youngerValue;
                        dating.younger_analysis_entity_id = ae.analysis_entity_id;
                    }
                }
            }
        }

        return dating;
    }

    async fetchDatingLab(site, datingLabId) {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }
        
        if(typeof site.lookup_tables.labs == "undefined") {
            site.lookup_tables.labs = [];
        }

        return pgClient.query("SELECT * FROM tbl_dating_labs WHERE dating_lab_id=$1", [datingLabId]).then(values => {
            let labMeta = values.rows[0];
            let foundLab = false;
            site.lookup_tables.labs.forEach(lab => {
                if(lab.dating_lab_id == datingLabId) {
                    foundLab = true;
                }
            });
            if(!foundLab) {
                site.lookup_tables.labs.push(labMeta);
            }
            this.app.releaseDbConnection(pgClient);
        });
    }

    getDataGroupByMethod(dataGroups, methodId) {
        for(let key in dataGroups) {
            if(dataGroups[key].method_id == methodId) {
                return dataGroups[key];
            }
        }
        return null;
    }

    postProcessSiteData(site) {
        //Here we are going to group the datasets which belong together (refers to the same analysis method) into "data_groups"
        let dataGroups = [];

        for(let dsKey in site.datasets) {
            let dataset = site.datasets[dsKey];

            for(let aeKey in dataset.analysis_entities) {
                let ae = dataset.analysis_entities[aeKey];

                //check here if we should be handling this dataset
                if(this.moduleMethodGroups.includes(dataset.method_group_id)) {
                    let dataGroup = this.getDataGroupByMethod(dataGroups, dataset.method_id);
                    if(dataGroup == null) {
                        dataGroup = {
                            method_id: dataset.method_id,
                            method_group_id: dataset.method_group_id,
                            data_points: [],
                            type: "dating_values",
                        };
                        dataGroups.push(dataGroup);
                    }

                    if(ae.dating_values) {
                        dataGroup.data_points.push(ae);
                    }
                }
            }
        }

        //now figure out the chronology overview
        this.fetchSiteTimeData(site);

        return site.data_groups = dataGroups.concat(site.data_groups);
    }

    async fetchSiteTimeData(site) {
        let siteDatingObject = {
            age_older: null,
            older_dataset_id: null,
            older_analysis_entity_id: null,
            age_younger: null,
            younger_dataset_id: null,
            younger_analysis_entity_id: null,
        }
        site.datasets.forEach(dataset => {
            let ages = this.getNormalizedDatingSpanFromDataset(dataset);
            if(ages.older > siteDatingObject.age_older || siteDatingObject.age_older == null) {
                siteDatingObject.age_older = ages.older;
                siteDatingObject.older_dataset_id = ages.dataset_id;
                siteDatingObject.older_analysis_entity_id = ages.older_analysis_entity_id;
            }
            if(ages.younger < siteDatingObject.age_younger || siteDatingObject.age_younger == null) {
                siteDatingObject.age_younger = ages.younger;
                siteDatingObject.younger_dataset_id = ages.dataset_id;
                siteDatingObject.younger_analysis_entity_id = ages.younger_analysis_entity_id;
            }
        });

        site.chronology_extremes = siteDatingObject;
    }
    
}

module.exports = DatingModule;