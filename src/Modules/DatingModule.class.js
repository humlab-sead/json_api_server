const fs = require('fs');

class DatingModule {
    constructor(app) {
        this.name = "Dating";
        this.moduleMethodGroups = [19, 20, 151, 3]; //should maybe include 21 as well...
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
    
    async fetchSiteData(site) {
        if(!this.siteHasModuleMethods(site)) {
            return site;
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
                            if(site.datasets[key].method_id == 151 || site.datasets[key].method_id == 148 || site.datasets[key].method_id == 38) { //this is horrible
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
                    }
                    queryPromises.push(promise);
                    
                });
            })
        });

        await Promise.all(queryPromises).then(() => {
            this.app.releaseDbConnection(pgClient);
        });

        return site;
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
        return site.data_groups = dataGroups.concat(site.data_groups);
    }
}

module.exports = DatingModule;