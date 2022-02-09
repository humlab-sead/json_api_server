class AbundanceModule {
    constructor(app) {
        this.name = "Abundance";
        this.moduleMethods = [3, 6, 8, 14, 15, 40, 111]; //include 60, 81?
        this.app = app;
        this.expressApp = this.app.expressApp;
        this.setupEndpoints();
    }

    setupEndpoints() {
    }

    siteHasModuleMethods(site) {
        for(let key in site.methods) {
            if(this.moduleMethods.includes(site.methods[key].method_id)) {
                return true;
            }
        }
        return false;
    }

    async fetchSiteData(site) {
        if(!this.siteHasModuleMethods(site)) {
            console.log("No abundance methods for site "+site.site_id);
            return site;
        }

        console.log("Fetching abundance data for site "+site.site_id);

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }
        
        let queryPromises = [];

        site.sample_groups.forEach(sampleGroup => {
            sampleGroup.physical_samples.forEach(physicalSample => {
                physicalSample.analysis_entities.forEach(analysisEntity => {
                    let promise = pgClient.query('SELECT * FROM tbl_abundances WHERE analysis_entity_id=$1', [analysisEntity.analysis_entity_id]).then(async data => {
                        analysisEntity.abundances = data.rows;

                        for(let key in analysisEntity.abundances) {
                            let abundance = analysisEntity.abundances[key];
                            //Fetch abundance identification levels
                            let sql = `
                            SELECT
                            tbl_abundance_ident_levels.abundance_id,
                            tbl_abundance_ident_levels.identification_level_id,
                            tbl_identification_levels.identification_level_abbrev,
                            tbl_identification_levels.identification_level_name,
                            tbl_identification_levels.notes
                            FROM tbl_abundance_ident_levels
                            LEFT JOIN tbl_identification_levels ON tbl_abundance_ident_levels.identification_level_id = tbl_identification_levels.identification_level_id
                            WHERE abundance_id=$1
                            `;
                            await pgClient.query(sql, [abundance.abundance_id]).then(identLevels => {
                                abundance.identification_levels = identLevels.rows;
                            });

                            //Fetch abundance elements
                            await pgClient.query('SELECT * FROM tbl_abundance_elements WHERE abundance_element_id=$1', [abundance.abundance_element_id]).then(abundanceElements => {
                                abundance.element = abundanceElements.rows[0];
                            });
                        }
                        
                    });
                    queryPromises.push(promise);
                });
            });
        });


        await Promise.all(queryPromises);
        this.app.releaseDbConnection(pgClient);
        
        return site;
    }

}

module.exports = AbundanceModule;