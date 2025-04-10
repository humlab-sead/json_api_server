class IsotopeModule {
    constructor(app) {
        this.name = "Isotope";
        this.moduleMethods = []; //should be 175 - but in the latest database release, method_id 175 was overwritten by the method for aDNA, so this module is not allowed to use this id for now. Not sure if isotopes will be method_id 175 again in the future, or how this will be solved, we will have to wait and see.
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
            console.log("Fetching "+this.name+" data for site "+site.site_id);
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = `
        SELECT 
        tbl_isotopes.isotope_id,
        tbl_isotopes.isotope_measurement_id,
        tbl_isotopes.measurement_value,
        tbl_isotopes.unit_id,
        tbl_isotopes.isotope_value_specifier_id,
        tbl_isotope_measurements.isotope_standard_id AS measurement_standard_id,
        tbl_isotope_measurements.method_id AS measurement_method_id,
        tbl_isotope_measurements.isotope_type_id AS measurement_isotope_type_id
        FROM tbl_isotopes
        LEFT JOIN tbl_isotope_value_specifiers ON tbl_isotopes.isotope_value_specifier_id=tbl_isotope_value_specifiers.isotope_value_specifier_id
        LEFT JOIN tbl_isotope_measurements ON tbl_isotope_measurements.isotope_measurement_id=tbl_isotopes.isotope_measurement_id
        WHERE analysis_entity_id=$1
        `;

        for(let key in site.sample_groups) {
            let sampleGroup = site.sample_groups[key];
            for(let key2 in sampleGroup.physical_samples) {
                let physicalSample = sampleGroup.physical_samples[key2];
                for(let key3 in physicalSample.analysis_entities) {
                    let analysisEntity = physicalSample.analysis_entities[key3];
                    let isotopes = await pgClient.query(sql, [analysisEntity.analysis_entity_id]);
                    analysisEntity.isotopes = isotopes.rows;

                    //things to fetch into lookup tables:
                    //isotope_value_specifier_id
                    //unit_id
                    //isotope_type_id

                    for(let key4 in isotopes.rows) {
                        let isoItem = isotopes.rows[key4];
                        if(isoItem.isotope_value_specifier_id) {
                            await this.fetchIsotopeValueSpecifiers(site, isoItem.isotope_value_specifier_id);
                        }
                        if(isoItem.measurement_isotope_type_id) {
                            await this.fetchIsotopeTypes(site, isoItem.measurement_isotope_type_id);
                        }

                        if(isoItem.measurement_standard_id) {
                            await this.fetchIsotopeStandard(site, isoItem.measurement_standard_id);
                        }
                        
                        if(isoItem.unit_id) {
                            let unit = this.app.getUnitByUnitId(site, isoItem.unit_id);
                            if(!unit) {
                                let unit = await this.app.fetchUnit(isoItem.unit_id);
                                if(unit) {
                                    this.app.addUnitToLocalLookup(site, unit);
                                }
                            }
                        }
                    }

                }
            }
        }

        await this.app.releaseDbConnection(pgClient);
        return site;
    }

    async fetchIsotopeStandard(site, isotope_standard_id) {
        if(typeof site.lookup_tables.isotope_standards == "undefined") {
            site.lookup_tables.isotope_standards = [];
        }

        let found = false;
        for(let i=0; i<site.lookup_tables.isotope_standards.length; i++) {
            if(site.lookup_tables.isotope_standards[i].isotope_standard_id == isotope_standard_id) {
                found = true;
                break;
            }
        }
        if(found) {
            //if this is already in the lookup table, don't fetch it again
            return;
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let isotopeStandard = null;

        let sql = `SELECT *
        FROM tbl_isotope_standards
        WHERE isotope_standard_id=$1
        `;
        let res = await pgClient.query(sql, [isotope_standard_id]);
        await this.app.releaseDbConnection(pgClient);
        if(res.rows.length > 0) {
            isotopeStandard = res.rows[0];
        }
        
        site.lookup_tables.isotope_standards.push(isotopeStandard);

        return isotopeStandard;
    }

    async fetchIsotopeTypes(site, isotope_type_id) {
            if(typeof site.lookup_tables.isotope_types == "undefined") {
                site.lookup_tables.isotope_types = [];
            }
    
            let found = false;
            for(let i=0; i<site.lookup_tables.isotope_types.length; i++) {
                if(site.lookup_tables.isotope_types[i].isotope_type_id == isotope_type_id) {
                    found = true;
                    break;
                }
            }
            if(found) {
                //if this is already in the lookup table, don't fetch it again
                return;
            }
    
            let pgClient = await this.app.getDbConnection();
            if(!pgClient) {
                return false;
            }
    
            let isotopeType = null;
    
            let sql = `SELECT *
            FROM tbl_isotope_types
            WHERE isotope_type_id=$1
            `;
            let res = await pgClient.query(sql, [isotope_type_id]);
            await this.app.releaseDbConnection(pgClient);
            if(res.rows.length > 0) {
                isotopeType = res.rows[0];
            }
            
            site.lookup_tables.isotope_types.push(isotopeType);
    
            return isotopeType;
    }

    async fetchIsotopeValueSpecifiers(site, isotope_value_specifier_id) {

        if(typeof site.lookup_tables.isotope_value_specifiers == "undefined") {
            site.lookup_tables.isotope_value_specifiers = [];
        }

        let found = false;
        for(let i=0; i<site.lookup_tables.isotope_value_specifiers.length; i++) {
            if(site.lookup_tables.isotope_value_specifiers[i].isotope_value_specifier_id == isotope_value_specifier_id) {
                found = true;
                break;
            }
        }
        if(found) {
            //if this is already in the lookup table, don't fetch it again
            return;
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let isotopeValueSpecifier = null;

        let sql = `SELECT *
        FROM tbl_isotope_value_specifiers
        WHERE isotope_value_specifier_id=$1
        `;
        let res = await pgClient.query(sql, [isotope_value_specifier_id]);
        await this.app.releaseDbConnection(pgClient);
        if(res.rows.length > 0) {
            isotopeValueSpecifier = res.rows[0];
        }
        
        site.lookup_tables.isotope_value_specifiers.push(isotopeValueSpecifier);

        return isotopeValueSpecifier;
    }

    datasetHasModuleMethods(dataset) {
        return this.moduleMethods.includes(dataset.method_id);
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
                    method_ids: [dataset.method_id],
                    biblio_ids: [],
                    id: dataset.dataset_id,
                    dataset_id: dataset.dataset_id,
                    dataset_name: dataset.dataset_name,
                    method_ids: [dataset.method_id],
                    method_group_ids: [dataset.method_group_id],
                    method_name: method.method_name,
                    type: "isotope",
                    values: []
                }

                let biblioIds = new Set();

                for(let aeKey in dataset.analysis_entities) {
                    let ae = dataset.analysis_entities[aeKey];
                    if(ae.dataset_id == dataGroup.id) {

                        for(let dsk in site.datasets) {
                            if(site.datasets[dsk].dataset_id == ae.dataset_id) {
                                let dataset = site.datasets[dsk];
                                if(dataset.biblio_id) {
                                    biblioIds.add(dataset.biblio_id);
                                }
                                break;
                            }
                        }

                        if(ae.isotopes) {
                            for(let dataKey in ae.isotopes) {
                                let isotopeData = ae.isotopes[dataKey];

                                let sampleName = this.app.getSampleNameBySampleId(site, ae.physical_sample_id);

                                dataGroup.values.push({
                                    analysis_entitity_id: ae.analysis_entity_id,
                                    physical_sample_id: ae.physical_sample_id,
                                    dataset_id: dataset.dataset_id,
                                    key: ae.physical_sample_id, 
                                    value: isotopeData,
                                    valueType: 'complex',
                                    data: isotopeData, //if 'complex' value type
                                    methodId: dataset.method_id,
                                    sample_name: sampleName,
                                });
                            }
                        }
                    }
                }

                dataGroup.biblio_ids = Array.from(biblioIds);

                dataGroups.push(dataGroup);
            }  
        }

        return site.data_groups = dataGroups.concat(site.data_groups);
    }
}

export default IsotopeModule;