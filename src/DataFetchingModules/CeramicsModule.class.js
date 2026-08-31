
class CeramicsModule {
    constructor(app) {
        this.name = "Ceramics";
        this.moduleMethods = [171, 172]; //not sure that 171 and 172 are the only methods relevant for ceramics... there could be more
        this.app = app;
        this.expressApp = this.app.expressApp;
    }

    siteHasModuleMethods(site) {
        for(let key in site.lookup_tables.methods) {
            if(this.moduleMethods.includes(site.lookup_tables.methods[key].method_id)) {
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
            console.log("Fetching ceramics data for site "+site.site_id);
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        try {
            //Fetched once for the whole site rather than once per analysis
            //entity, then grouped and consumed by the loops below unchanged.
            const analysisEntityIds = [];
            site.sample_groups.forEach(sampleGroup => {
                sampleGroup.physical_samples.forEach(physicalSample => {
                    physicalSample.analysis_entities.forEach(analysisEntity => {
                        analysisEntityIds.push(analysisEntity.analysis_entity_id);
                    });
                });
            });

            const ceramicValuesByAnalysisEntity = new Map();
            if(analysisEntityIds.length > 0) {
                const ceramicValues = await pgClient.query(`
                    SELECT * FROM tbl_ceramics
                    INNER JOIN tbl_ceramics_lookup ON tbl_ceramics_lookup.ceramics_lookup_id=tbl_ceramics.ceramics_lookup_id
                    WHERE analysis_entity_id = ANY($1::bigint[])
                    ORDER BY analysis_entity_id, tbl_ceramics.ceramics_id
                    `, [analysisEntityIds]);
                ceramicValues.rows.forEach(row => {
                    const key = String(row.analysis_entity_id);
                    if(!ceramicValuesByAnalysisEntity.has(key)) {
                        ceramicValuesByAnalysisEntity.set(key, []);
                    }
                    ceramicValuesByAnalysisEntity.get(key).push(row);
                });
            }

            let dataGroupId = 1;
            let dataGroups = [];

            for(let sgKey in site.sample_groups) {
                let sampleGroup = site.sample_groups[sgKey];
                for(let sampleKey in sampleGroup.physical_samples) {
                    let physicalSample = sampleGroup.physical_samples[sampleKey];

                    let dataGroup = {
                        data_group_id: dataGroupId++,
                        physical_sample_id: physicalSample.physical_sample_id,
                        biblio_ids: [],
                        method_ids: [],
                        method_group_ids: [],
                        values: []
                    };

                    let biblioIds = new Set();
                    let methodIds = new Set();
                    let methodGroupIds = new Set();
                    for(let key in physicalSample.analysis_entities) {
                        let analysisEntity = physicalSample.analysis_entities[key];

                        for(let dsk in site.datasets) {
                            if(site.datasets[dsk].dataset_id == analysisEntity.dataset_id) {
                                let dataset = site.datasets[dsk];
                                if(dataset.biblio_id) {
                                    biblioIds.add(dataset.biblio_id);
                                }
                                break;
                            }
                        }

                        analysisEntity.ceramic_values = ceramicValuesByAnalysisEntity.get(String(analysisEntity.analysis_entity_id)) || [];
                        dataGroup.analysis_entity_id = analysisEntity.analysis_entity_id;

                        analysisEntity.ceramic_values.forEach(ceramicValue => {
                            dataGroup.values.push({
                                key: ceramicValue.name,
                                value: ceramicValue.measurement_value,
                                valueType: 'simple',
                                //data: ceramicValue,
                                methodId: ceramicValue.method_id,
                                ceramicsId: ceramicValue.ceramics_id,
                                ceramicsLookupId: ceramicValue.ceramics_lookup_id,
                                description: ceramicValue.description,
                            });

                            methodIds.add(ceramicValue.method_id);
                            methodGroupIds.add(ceramicValue.method_group_id);
                        })
                    }

                    dataGroup.biblio_ids = Array.from(biblioIds);
                    dataGroup.method_ids = Array.from(methodIds);
                    dataGroup.method_group_ids = Array.from(methodGroupIds);

                    dataGroups.push(dataGroup);
                }
            }

            site.data_groups = site.data_groups.concat(dataGroups);
        }
        finally {
            await this.app.releaseDbConnection(pgClient);
        }

        return site;
    }

    postProcessSiteData(site) {
        return site;
    }
}

export default CeramicsModule;
