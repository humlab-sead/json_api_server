class MeasuredValuesModule {
    constructor(app) {
        this.name = "MeasuredValues";
        this.moduleMethodGroups = [2];
        this.app = app;
        this.expressApp = this.app.expressApp;
        this.setupEndpoints();
    }

    siteHasModuleMethods(site) {
        for(let key in site.methods) {
            if(this.moduleMethodGroups.includes(site.methods[key].method_group_id)) {
                return true;
            }
        }
        return false;
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

}

module.exports = MeasuredValuesModule;