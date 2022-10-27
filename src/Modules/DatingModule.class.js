class DatingModule {
    constructor(app) {
        this.name = "Dating";
        this.moduleMethodGroups = [19, 20];
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

        let queryPromises = [];
        site.sample_groups.forEach(sampleGroup => {
            sampleGroup.physical_samples.forEach(physicalSample => {
                physicalSample.analysis_entities.forEach(analysisEntity => {
                    let promise = pgClient.query(sql, [analysisEntity.analysis_entity_id]).then(values => {
                        analysisEntity.dating_values = values.rows[0];
                    });
                    queryPromises.push(promise);
                });
            })
        });

        await Promise.all(queryPromises).then(() => {
            this.app.releaseDbConnection(pgClient);
        });

        return site;
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
        return site.data_groups = dataGroups.concat(site.data_groups);
    }
}

module.exports = DatingModule;