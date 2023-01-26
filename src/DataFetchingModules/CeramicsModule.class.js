class CeramicsModule {
    constructor(app) {
        this.name = "Ceramics";
        this.moduleMethods = [171, 172]; //not sure that 171 and 172 are the only methods relevant for ceramics... there could be more
        this.app = app;
        this.expressApp = this.app.expressApp;
    }

    siteHasModuleMethods(site) {
        for(let key in site.lookup_tables.analysis_methods) {
            if(this.moduleMethods.includes(site.lookup_tables.analysis_methods[key].method_id)) {
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
        SELECT * FROM tbl_ceramics 
        INNER JOIN tbl_ceramics_lookup ON tbl_ceramics_lookup.ceramics_lookup_id=tbl_ceramics.ceramics_lookup_id
        WHERE analysis_entity_id=$1
        `;

        let queryPromises = [];
        site.sample_groups.forEach(sampleGroup => {
            sampleGroup.physical_samples.forEach(physicalSample => {
                physicalSample.analysis_entities.forEach(analysisEntity => {
                    let promise = pgClient.query(sql, [analysisEntity.analysis_entity_id]).then(ceramicValues => {
                        analysisEntity.ceramic_values = ceramicValues.rows;
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
        return site;
    }
}

module.exports = CeramicsModule;