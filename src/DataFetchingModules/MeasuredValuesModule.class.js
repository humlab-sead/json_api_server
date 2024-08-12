class MeasuredValuesModule {
    constructor(app) {
        this.name = "MeasuredValues";
        this.moduleMethodGroups = [2];
        this.app = app;
        this.expressApp = this.app.expressApp;
        this.setupEndpoints();
    }

    siteHasModuleMethods(site) {
        for(let key in site.lookup_tables.methods) {
            if(this.moduleMethodGroups.includes(site.lookup_tables.methods[key].method_group_id)) {
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

    async fetchSiteData(site, verbose = false) {
        if(!this.siteHasModuleMethods(site)) {
            return site;
        }

        if(verbose) {
            console.log("Fetching measured values data for site "+site.site_id);
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

        let dataGroups = [];

        for(let dsKey in site.datasets) {
            let dataset = site.datasets[dsKey];
            if(this.datasetHasModuleMethods(dataset)) {

                let method = this.app.getMethodByMethodId(site, dataset.method_id);

                let dataGroup = {
                    data_group_id: dataset.dataset_id,
                    physical_sample_id: null,
                    id: dataset.dataset_id,
                    dataset_name: dataset.dataset_name,
                    method_ids: [dataset.method_id],
                    method_group_ids: [dataset.method_group_id],
                    method_name: method.method_name,
                    type: "measured_values",
                    values: []
                }

                for(let aeKey in dataset.analysis_entities) {
                    let ae = dataset.analysis_entities[aeKey];
                    if(ae.dataset_id == dataGroup.id) {
                        if(ae.measured_values) {
                            for(let mvKey in ae.measured_values) {
                                let measuredValue = ae.measured_values[mvKey];
                                let value = measuredValue.measured_value;

                                if(!isNaN(parseFloat(value))) {
                                    value = parseFloat(value);
                                }
                                else if(!isNaN(parseInt(value))) {
                                    value = parseInt(value);
                                }

                                dataGroup.values.push({
                                    analysis_entitity_id: ae.analysis_entity_id,
                                    dataset_id: ae.dataset_id,
                                    key: ae.physical_sample_id,
                                    physical_sample_id: ae.physical_sample_id, 
                                    value: value,
                                    valueType: 'simple',
                                    data: measuredValue.measured_value,
                                    methodId: dataset.method_id,
                                    sample_name: this.app.getSampleNameBySampleId(site, ae.physical_sample_id),
                                    prep_methods: ae.prepMethods,
                                    /* OLD:
                                    physical_sample_id: ae.physical_sample_id,
                                    sample_name: this.app.getSampleNameBySampleId(site, ae.physical_sample_id),
                                    label: ae.physical_sample_id,
                                    raw_value: measuredValue.measured_value,
                                    value: value,
                                    dataset_id: ae.dataset_id
                                    */
                                });
                            }
                        }
                    }
                }

                dataGroups.push(dataGroup);
            }  
        }
        return site.data_groups = dataGroups.concat(site.data_groups);
    }

}

export default MeasuredValuesModule;