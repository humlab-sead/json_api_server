class MeasuredValuesModule {
    constructor(app) {
        this.name = "MeasuredValues";
        this.moduleMethodGroups = [2];
        this.app = app;
        this.expressApp = this.app.expressApp;
        this.setupEndpoints();
    }

    siteHasModuleMethods(site) {
        for(let key in site.analysis_methods) {
            if(this.moduleMethodGroups.includes(site.analysis_methods[key].method_group_id)) {
                return true;
            }
        }
        return false;
    }

    datasetHasModuleMethods(dataset) {
        //If this dataset is a measured_values dataset
        return this.moduleMethodGroups.includes(dataset.method_group_id);
    }

    setupEndpoints() {
    }

    async fetchSiteData(site) {
        if(!this.siteHasModuleMethods(site)) {
            return site;
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let queryPromises = [];
        site.sample_groups.forEach(sampleGroup => {
            sampleGroup.physical_samples.forEach(physicalSample => {
                physicalSample.analysis_entities.forEach(analysisEntity => {
                    ;
                    let promise = pgClient.query('SELECT * FROM tbl_measured_values WHERE analysis_entity_id=$1', [analysisEntity.analysis_entity_id]).then(measuredValues => {
                        analysisEntity.measured_values = measuredValues.rows;
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

    postProcessSiteData(site) {

        //Populate datasets with analysis_entities and their measured values
        /*
        for(let dsKey in site.datasets) {
            let dataset = site.datasets[dsKey];
            if(this.datasetHasModuleMethods(dataset)) {
                dataset.values = [];
                for(let sgKey in site.sample_groups) {
                    let sampleGroup = site.sample_groups[sgKey];
                    for(let sampleKey in sampleGroup.physical_samples) {
                        let sample = sampleGroup.physical_samples[sampleKey];
                        for(let aeKey in sample.analysis_entities) {
                            let ae = sample.analysis_entities[aeKey];
                            if(ae.dataset_id == dataset.dataset_id) {
                                if(ae.measured_values) {
                                    for(let mvKey in ae.measured_values) {
                                        let measuredValue = ae.measured_values[mvKey];
                                        let value = measuredValue.measured_value;
                                        dataset.values.push({
                                            physical_sample_id: sample.physical_sample_id,
                                            sample_name: sample.sample_name,
                                            value: value
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        */
        return site;
    }

}

module.exports = MeasuredValuesModule;