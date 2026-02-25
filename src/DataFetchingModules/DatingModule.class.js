import DendroLib from '../Lib/sead_common/DendroLib.class.js';
import SiteDatingSummary from '../Models/SiteDatingSummary.class.js';

class DatingModule {
    constructor(app) {
        this.name = "Dating";
        this.moduleMethods = [
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
            174,
            176
        ];

        /*
        this.moduleMethods = [
            174, 6, 8, 3, 14, 40, 15, 10, 154, 127, 38, 39, 151, 146, 128, 129, 130,
            131, 142, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 143, 144, 145,
            147, 148, 149, 152, 153, 155, 111, 156, 157, 158, 159, 160, 161, 162, 163,
            164, 165, 166, 167, 168, 169, 170, 175
        ];
        */
          

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

    async fetchRelativeAgesData(site, analysisEntity) {
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

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let values = await pgClient.query(sql, [analysisEntity.analysis_entity_id]);
        this.app.releaseDbConnection(pgClient);

        return values.rows[0];
    }

    async fetchGeoChronologyData(site, analysisEntity) {
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

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let values = await pgClient.query(c14stdSql, [analysisEntity.analysis_entity_id]);
        this.app.releaseDbConnection(pgClient);
        return values.rows[0];
    }
    
    async fetchAnalysisEntityAgesData(site, analysisEntity) {
        let entityAgesSql = `
        SELECT * FROM tbl_analysis_entity_ages
        WHERE analysis_entity_id=$1`;

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        const values = await pgClient.query(entityAgesSql, [analysisEntity.analysis_entity_id]);
        this.app.releaseDbConnection(pgClient);

        return values.rows[0];
    }

    async fetchSiteData(site, verbose = false) {
        if(!this.siteHasModuleMethods(site)) {
            return site;
        }

        if(verbose) {
            console.log("Fetching dating data for site "+site.site_id);
        }

        let extremes = {
            older: null,
            older_type: null,
            older_analysis_entity_id: null,
            younger: null,
            younger_type: null,
            younger_analysis_entity_id: null,
        }

        for (let sampleGroup of site.sample_groups) {
            for (let physicalSample of sampleGroup.physical_samples) {
                for (let analysisEntity of physicalSample.analysis_entities) {
                    //Here we need to check what dataset this AE is linked to
                    //if it is a dataset with method_id 151 (among others) then it's 'C14 std'
                    //which needs to be handled/fetched differently from the other dating methods
                    let c14StdDataset = false;
                    for(let key in site.datasets) {
                        if(analysisEntity.dataset_id == site.datasets[key].dataset_id) {
                            if(this.c14StdMethodIds.includes(site.datasets[key].method_id)) {
                                c14StdDataset = true;
                            }
                        }
                    }

                    if(physicalSample.sample_name == "F115" || true) {

                        console.log("Processing analysis entity "+analysisEntity.analysis_entity_id+" for sample F115");

                        if(c14StdDataset) {
                            const geochronologyData = await this.fetchGeoChronologyData(site, analysisEntity);
                            //console.log("Fetched geochronology data:");
                            //console.log(geochronologyData);
                            
                            if(geochronologyData && parseInt(geochronologyData.age)) {
                                let older = parseInt(geochronologyData.age) - parseInt(geochronologyData.error_older);
                                let younger = parseInt(geochronologyData.age) + parseInt(geochronologyData.error_younger);

                                if(extremes.older == null || older < extremes.older) {
                                    extremes.older = older;
                                    extremes.older_type = "c14std";
                                    extremes.older_analysis_entity_id = analysisEntity.analysis_entity_id;
                                }
                                if(extremes.younger == null || younger > extremes.younger) {
                                    extremes.younger = younger;
                                    extremes.younger_type = "c14std";
                                    extremes.younger_analysis_entity_id = analysisEntity.analysis_entity_id;
                                }

                                analysisEntity.geochronologyData = geochronologyData;
                            }

                            //if dating_lab_id seems to be a number and we don't already have his lab 
                            //among the lookups, add it
                            const datingLabId = parseInt(geochronologyData.dating_lab_id);
                            if(datingLabId != NaN) {
                                await this.fetchDatingLab(site, datingLabId);
                            }
                        }
                        else {
                            const relativeAgesdata = await this.fetchRelativeAgesData(site, analysisEntity);
                            //console.log("Fetched relative ages data:");
                            //console.log(relativeAgesdata);

                            if(relativeAgesdata && parseInt(relativeAgesdata.cal_age_older) && parseInt(relativeAgesdata.cal_age_younger)) {
                                if(extremes.older == null || parseInt(relativeAgesdata.cal_age_older) < extremes.older) {
                                    extremes.older = parseInt(relativeAgesdata.cal_age_older);
                                    extremes.older_type = "relative";
                                    extremes.older_analysis_entity_id = analysisEntity.analysis_entity_id;
                                
                                }
                                if(extremes.younger == null || parseInt(relativeAgesdata.cal_age_younger) > extremes.younger) {
                                    extremes.younger = parseInt(relativeAgesdata.cal_age_younger);
                                    extremes.younger_type = "relative";
                                    extremes.younger_analysis_entity_id = analysisEntity.analysis_entity_id;
                                }

                                analysisEntity.relativeAgesdata = relativeAgesdata;
                            }

                            if(relativeAgesdata && parseInt(relativeAgesdata.c14_age_older) && parseInt(relativeAgesdata.c14_age_younger)) {
                                if(extremes.older == null || parseInt(relativeAgesdata.c14_age_older) < extremes.older) {
                                    extremes.older = parseInt(relativeAgesdata.c14_age_older);
                                    extremes.older_type = "relative_c14";
                                    extremes.older_analysis_entity_id = analysisEntity.analysis_entity_id;
                                }
                                if(extremes.younger == null || parseInt(relativeAgesdata.c14_age_younger) > extremes.younger) {
                                    extremes.younger = parseInt(relativeAgesdata.c14_age_younger);
                                    extremes.younger_type = "relative_c14";
                                    extremes.younger_analysis_entity_id = analysisEntity.analysis_entity_id;
                                }
                            }

                        }
                        const analysisEntityAgesData = await this.fetchAnalysisEntityAgesData(site, analysisEntity);
                        //console.log("Fetched analysis entity ages data:");
                        //console.log(analysisEntityAgesData);

                        if(analysisEntityAgesData && parseInt(analysisEntityAgesData.age_older) && parseInt(analysisEntityAgesData.age_younger)) {
                            if(extremes.older == null || parseInt(analysisEntityAgesData.age_older) < extremes.older) {
                                extremes.older = parseInt(analysisEntityAgesData.age_older);
                                extremes.older_type = "entityAges";
                                extremes.older_analysis_entity_id = analysisEntity.analysis_entity_id;
                            }
                            if(extremes.younger == null || parseInt(analysisEntityAgesData.age_younger) > extremes.younger) {
                                extremes.younger = parseInt(analysisEntityAgesData.age_younger);
                                extremes.younger_type = "entityAges";
                                extremes.younger_analysis_entity_id = analysisEntity.analysis_entity_id;
                            }

                            analysisEntity.analysisEntityAgesData = analysisEntityAgesData;
                        }
                    }
                    
                }
            }
        }

        site.dating_summary = extremes;

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
            dataset_id: dataset.dataset_id,
            older_type: null,
            younger_type: null,
            older_analysis_entity_id: null,
            younger_analysis_entity_id: null,
        };
    
        for (let aeKey in dataset.analysis_entities) {
            let ae = dataset.analysis_entities[aeKey];
            if (ae.dating_values) {
                let datingValues = ae.dating_values;
    
                //c14
                if (datingValues.c14_age_older && datingValues.c14_age_younger) {
                    let olderValue = parseInt(datingValues.c14_age_older);
                    let youngerValue = parseInt(datingValues.c14_age_younger);
    
                    if (olderValue > dating.older || dating.older == null) {
                        dating.older = olderValue;
                        dating.older_type = 'c14';
                        dating.older_analysis_entity_id = ae.analysis_entity_id;
                    }
                    if (youngerValue < dating.younger || dating.younger == null) {
                        dating.younger = youngerValue;
                        dating.younger_type = 'c14';
                        dating.younger_analysis_entity_id = ae.analysis_entity_id;
                    }
                }
    
                //cal - Assuming cal_age is in calendar years (adjust logic based on actual unit)
                if (datingValues.cal_age_older && datingValues.cal_age_younger) {
                    let olderValue = parseInt(datingValues.cal_age_older);
                    let youngerValue = parseInt(datingValues.cal_age_younger);
    
                    // Need to know if larger cal_age is older or younger!
                    // Assuming larger is older for this example - ADJUST IF WRONG
                    if (olderValue > dating.older || dating.older == null) {
                        dating.older = olderValue;
                        dating.older_type = 'cal';
                        dating.older_analysis_entity_id = ae.analysis_entity_id;
                    }
                    if (youngerValue < dating.younger || dating.younger == null) {
                        dating.younger = youngerValue;
                        dating.younger_type = 'cal';
                        dating.younger_analysis_entity_id = ae.analysis_entity_id;
                    }
                }
    
                //other
                if (datingValues.age && datingValues.error_older && datingValues.error_younger) {
                    let olderValue = parseInt(datingValues.age) - parseInt(datingValues.error_older);
                    let youngerValue = parseInt(datingValues.age) + parseInt(datingValues.error_younger);
    
                    if (olderValue > dating.older || dating.older == null) {
                        dating.older = olderValue;
                        dating.older_type = 'other';
                        dating.older_analysis_entity_id = ae.analysis_entity_id;
                    }
                    if (youngerValue < dating.younger || dating.younger == null) {
                        dating.younger = youngerValue;
                        dating.younger_type = 'other';
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
                    biblio_ids: [],
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

                let biblioIds = new Set();
                let analysisEntitiesSet = new Set();
                let physicalSampleIdsSet = new Set();

                for(let aeKey in dataset.analysis_entities) {
                    let ae = dataset.analysis_entities[aeKey];
                    if(ae.dataset_id == dataGroup.dataset_id) {
                        if(ae.dating_values) {
                            for(let dKey in ae.dating_values) {
                                analysisEntitiesSet.add(ae.analysis_entity_id);
                                physicalSampleIdsSet.add(ae.physical_sample_id);

                                for(let dsk in site.datasets) {
                                    if(site.datasets[dsk].dataset_id == ae.dataset_id) {
                                        let dataset = site.datasets[dsk];
                                        if(dataset.biblio_id) {
                                            biblioIds.add(dataset.biblio_id);
                                        }
                                        break;
                                    }
                                }
                                let sampleName = this.app.getSampleNameBySampleId(site, ae.physical_sample_id);

                                dataGroup.values.push({
                                    analysis_entity_id: ae.analysis_entity_id,
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
                        if(ae.analysisEntityAgesData) {

                            analysisEntitiesSet.add(ae.analysis_entity_id);
                            physicalSampleIdsSet.add(ae.physical_sample_id);
                            let sampleName = this.app.getSampleNameBySampleId(site, ae.physical_sample_id);

                            const addValueToDataGroup = (key, value) => {
                                if (value) {
                                    dataGroup.values.push({
                                        analysis_entity_id: ae.analysisEntityAgesData.analysis_entity_id,
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
                            
                            addValueToDataGroup('age', ae.analysisEntityAgesData.age);
                            addValueToDataGroup('age_older', ae.analysisEntityAgesData.age_older);
                            addValueToDataGroup('age_younger', ae.analysisEntityAgesData.age_younger);
                            addValueToDataGroup('age_range', ae.analysisEntityAgesData.age_range);
                            addValueToDataGroup('chronology_id', ae.analysisEntityAgesData.chronology_id);
                            addValueToDataGroup('dating_specifier', ae.analysisEntityAgesData.dating_specifier);
                            
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

                dataGroup.biblio_ids = Array.from(biblioIds);

                dataGroups.push(dataGroup);
            }  
        }

        return site.data_groups = dataGroups.concat(site.data_groups);
    }

    /**
     * This method will fetch a summary of the dating data for a given site. It only returns the extremes.
     * 
     * 
     * @param {*} site 
     * @returns 
     */
    async fetchSiteTimeData(site) {

        let siteDatingSummary = new SiteDatingSummary();

        /*
        siteDatingSummary = {
            age_older: null,
            older_dataset_id: null,
            older_analysis_entity_id: null,
            older_type: null,
            age_younger: null,
            younger_dataset_id: null,
            younger_analysis_entity_id: null,
            younger_type: null,
        };
        */

        let dendroDatings = this.getDendroDatingExtremes(site);

        if(dendroDatings.age_older && (dendroDatings.age_older < siteDatingSummary.age_older || siteDatingSummary.age_older == null)) {
            siteDatingSummary.age_older = dendroDatings.age_older;
            siteDatingSummary.older_dataset_id = dendroDatings.older_dataset_id;
            siteDatingSummary.older_analysis_entity_id = dendroDatings.older_analysis_entity_id;
            siteDatingSummary.older_type = dendroDatings.older_type;
        }
        if(dendroDatings.age_younger && (dendroDatings.age_younger > siteDatingSummary.age_younger || siteDatingSummary.age_younger == null)) {
            siteDatingSummary.age_younger = dendroDatings.age_younger;
            siteDatingSummary.younger_dataset_id = dendroDatings.younger_dataset_id;
            siteDatingSummary.younger_analysis_entity_id = dendroDatings.younger_analysis_entity_id;
            siteDatingSummary.younger_type = dendroDatings.younger_type;
        }

        //TODO: implement the rest of the dating methods here, like C14, etc.
        site.datasets.forEach(dataset => {
            let datingSummary = this.getNormalizedDatingSpanFromDataset(dataset);
            if(datingSummary.dating_range_age_type_id == 1) { //dating_range_age_type_id is an "AD" dating
                if(datingSummary.dating_range_low_value < siteDatingSummary.age_older || siteDatingSummary.age_older == null) {
                    siteDatingSummary.age_older = datingSummary.dating_range_low_value;
                    siteDatingSummary.older_dataset_id = datingSummary.dataset_id;
                    siteDatingSummary.older_analysis_entity_id = datingSummary.analysis_entity_id;
                    // Assuming datingSummary has older_type
                    siteDatingSummary.older_type = datingSummary.older_type;
                }
    
                if(datingSummary.dating_range_high_value > siteDatingSummary.age_younger || siteDatingSummary.age_younger == null) {
                    siteDatingSummary.age_younger = datingSummary.dating_range_high_value;
                    siteDatingSummary.younger_dataset_id = datingSummary.dataset_id;
                    siteDatingSummary.younger_analysis_entity_id = datingSummary.analysis_entity_id;
                    // Assuming datingSummary has younger_type
                    siteDatingSummary.younger_type = datingSummary.younger_type;
                }
            }
            else {
                //console.warn("This dataset has a dating range type ("+datingSummary.dating_range_age_type_id+") that is not AD, so it will not be included in the site dating summary");
            }
        });

        return siteDatingSummary;
    }

    getDendroDatingExtremes(site) {
        let siteDatingObject = {
            age_older: null,
            older_dataset_id: null,
            older_analysis_entity_id: null,
            older_type: null,
            age_younger: null,
            younger_dataset_id: null,
            younger_analysis_entity_id: null,
            younger_type: null,
        };

        let dl = new DendroLib();

        let dendroDataGroups = site.data_groups.filter(dataGroup => {
            return dataGroup.method_ids.includes(10); // Assuming 10 is the method ID for dendro
        });
        let sampleDataObjects = dl.dataGroupsToSampleDataObjects(dendroDataGroups);

        sampleDataObjects.forEach(sampleDataObject => {
            let oldest = dl.getYoungestGerminationYear(sampleDataObject);
            let youngest = dl.getYoungestFellingYear(sampleDataObject);

            //compare against the siteDatingObject
            if (oldest && oldest.value && (oldest.value < siteDatingObject.age_older || siteDatingObject.age_older == null)) {
                siteDatingObject.age_older = oldest.value;
                siteDatingObject.older_dataset_id = sampleDataObject.dataset_id;
                // Potentially fetch and assign older_analysis_entity_id if relevant for dendro
                siteDatingObject.older_type = "dendro";
            }
            if (youngest && youngest.value && (youngest.value > siteDatingObject.age_younger || siteDatingObject.age_younger == null)) {
                siteDatingObject.age_younger = youngest.value;
                siteDatingObject.younger_dataset_id = sampleDataObject.dataset_id;
                // Potentially fetch and assign younger_analysis_entity_id if relevant for dendro
                siteDatingObject.younger_type = "dendro";
            }
        });

        return siteDatingObject;
    }

}

export default DatingModule;