import DendroLib from '../Lib/sead_common/DendroLib.class.js';

class DatingModule {
    constructor(app) {
        this.name = "Dating";
        this.moduleMethods = [
            10,
            14,
            38,
            39,
            127,
            128,
            129,
            130,
            131,
            132,
            133,
            134,
            135,
            136,
            137,
            138,
            139,
            140,
            141,
            142,
            143,
            144,
            146,
            147,
            148,
            149,
            151,
            152,
            153,
            154,
            155,
            156,
            157,
            158,
            159,
            160,
            161,
            162,
            163,
            164,
            165,
            167,
            168,
            169,
            170,
            176
        ];
        //this.moduleMethods = [38, 162, 163, 164, 165, 167, 168, 169, 170, 146, 39, 151, 154, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 147, 148, 149, 152, 153, 155, 10, 159, 161, 156, 157, 158];
        this.moduleMethodGroups = [19, 20, 3]; //should maybe include 21 as well... and also I added the individual methods to the moduleMethods array, because it makes some things easier, so this might be slightly redundant now
        this.c14StdMethodIds = [151, 148, 38, 150, 152, 160];
        this.entityAgesMethods = [];
        this.app = app;
        this.expressApp = this.app.expressApp;
    }

    siteHasModuleMethods(site) {
        for(let key in site.lookup_tables.methods) {
            if(this.moduleMethodGroups.includes(site.lookup_tables.methods[key].method_group_id)) {
                return true;
            }
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

        let entityAgesSql = `
        SELECT * FROM tbl_analysis_entity_ages
        WHERE analysis_entity_id=$1`; //this is already implemented in fetchAnalysisEntitiesAges() method, but that is for creating a site wide age summary
        
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

                    pgClient.query(entityAgesSql, [analysisEntity.analysis_entity_id]).then(values => {
                        analysisEntity.entity_ages = values.rows[0];
                    });

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
            dataGroup.values.forEach(value => {
                older = parseInt(value.dating_values.age) - parseInt(value.dating_values.error_older);
                younger = parseInt(value.dating_values.cal_age_younger) + parseInt(value.dating_values.error_younger);
            });
        }
        else {
            type = "modern";
            dataGroup.values.forEach(value => {
                older = parseInt(value.dating_values.cal_age_older);
                younger = parseInt(value.dating_values.cal_age_younger);
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
                    id: dataset.dataset_id,
                    dataset_id: dataset.dataset_id,
                    dataset_name: dataset.dataset_name,
                    method_ids: [dataset.method_id],
                    method_group_ids: [dataset.method_group_id],
                    method_group_id: dataset.method_group_id,
                    method_name: method.method_name,
                    type: "dating",
                    values: []
                }

                let analysisEntitiesSet = new Set();
                let physicalSampleIdsSet = new Set();

                for(let aeKey in dataset.analysis_entities) {
                    let ae = dataset.analysis_entities[aeKey];
                    if(ae.dataset_id == dataGroup.dataset_id) {
                        if(ae.dating_values) {
                            for(let dKey in ae.dating_values) {
                                analysisEntitiesSet.add(ae.analysis_entity_id);
                                physicalSampleIdsSet.add(ae.physical_sample_id);
                                let sampleName = this.app.getSampleNameBySampleId(site, ae.physical_sample_id);

                                dataGroup.values.push({
                                    analysis_entitity_id: ae.analysis_entity_id,
                                    dataset_id: dataset.dataset_id,
                                    key: dKey, 
                                    value: ae.dating_values[dKey],
                                    valueType: 'complex',
                                    data: ae.dating_values[dKey],
                                    methodId: dataset.method_id,
                                    physical_sample_id: ae.physical_sample_id,
                                    sample_name: sampleName,
                                });
                            }
                        }
                        if(ae.entity_ages) {

                            analysisEntitiesSet.add(ae.analysis_entity_id);
                            physicalSampleIdsSet.add(ae.physical_sample_id);
                            let sampleName = this.app.getSampleNameBySampleId(site, ae.physical_sample_id);

                            const addValueToDataGroup = (key, value) => {
                                if (value) {
                                    dataGroup.values.push({
                                        analysis_entity_id: ae.entity_ages.analysis_entity_id,
                                        dataset_id: dataset.dataset_id,
                                        key: key,
                                        value: value,
                                        valueType: 'simple',
                                        data: value,
                                        methodId: dataset.method_id,
                                        physical_sample_id: ae.physical_sample_id,
                                        sample_name: sampleName,
                                    });
                                }
                            };

                            addValueToDataGroup('age', ae.entity_ages.age);
                            addValueToDataGroup('age_older', ae.entity_ages.age_older);
                            addValueToDataGroup('age_younger', ae.entity_ages.age_younger);
                            addValueToDataGroup('age_range', ae.entity_ages.age_range);
                            addValueToDataGroup('chronology_id', ae.entity_ages.chronology_id);
                            addValueToDataGroup('dating_specifier', ae.entity_ages.dating_specifier);
                            
                        }
                    }
                }

                //if set only contains one item
                if(analysisEntitiesSet.size == 1) {
                    dataGroup.analysis_entity_id = analysisEntitiesSet.values().next().value;
                }
                if(physicalSampleIdsSet.size == 1) {
                    dataGroup.physical_sample_id = physicalSampleIdsSet.values().next().value;
                }

                dataGroups.push(dataGroup);
            }  
        }

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

        //if this is a dendro site...
        let dl = new DendroLib();
        site.data_groups.forEach(dataGroup => {
            if(dataGroup.type == "dendro") {
                let oldestGerminationYear = dl.getOldestGerminationYear(dataGroup);
                if(!oldestGerminationYear || !oldestGerminationYear.value) {
                    oldestGerminationYear = dl.getYoungestGerminationYear(dataGroup);
                }
                let youngestFellingYear = dl.getYoungestFellingYear(dataGroup);
                if(!youngestFellingYear || !youngestFellingYear.value) {
                    youngestFellingYear = dl.getOldestFellingYear(dataGroup);
                }

                if(oldestGerminationYear.value != null && (oldestGerminationYear.value < siteDatingObject.age_older || siteDatingObject.age_older == null)) {
                    siteDatingObject.age_older = oldestGerminationYear.value;
                    siteDatingObject.older_dataset_id = dataGroup.dataset_id;
                    siteDatingObject.date_type = "dendro";
                }
                if(youngestFellingYear.value != null && (youngestFellingYear.value > siteDatingObject.age_younger || siteDatingObject.age_younger == null)) {
                    siteDatingObject.age_younger = youngestFellingYear.value;
                    siteDatingObject.younger_dataset_id = dataGroup.dataset_id;
                    siteDatingObject.date_type = "dendro";
                }
            }
        });

        /*
        //if date type is dendro, then we need to recalculate this to BP years
        if(siteDatingObject.date_type == "dendro") {
            const currentYear = new Date().getFullYear();
            const diff = currentYear - 1950; // Calculate the difference from 1950 to the current year
            
            if (siteDatingObject.age_older !== null) {
                siteDatingObject.age_older = siteDatingObject.age_older - diff;
            }
            if (siteDatingObject.age_younger !== null) {
                siteDatingObject.age_younger = siteDatingObject.age_younger - diff;
            }
            //FTURE ME: PLEASE CHECK THAT THIS BP CALC IS CORRECT :D
        }
        */
        site.chronology_extremes = siteDatingObject;
        return siteDatingObject;
    }
    
}

export default DatingModule;