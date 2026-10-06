import DendroLib from '../Lib/sead_common/DendroLib.class.js';

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
        //Geochronology methods whose ages are conventional radiocarbon years, i.e. not calibrated
        this.radiocarbonYearsMethodIds = [38, 148, 151, 153];
        this.dendroMethodId = 10;
        //"Present" in years before present (BP)
        this.bpYear = 1950;
        //relative_age_type_id of "Calendar date" - a single date, so a missing younger value is not an open end
        this.calendarDateAgeTypeId = 12;
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
        //Not limited to sites with dating methods (siteHasModuleMethods), since analyses of other methods can be
        //dated too, e.g. ceramics with relative dates, and a site with only e.g. thermoluminescence would be missed.
        if(verbose) {
            console.log("Fetching dating data for site "+site.site_id);
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        try {
            let sql = `
            SELECT
            tbl_relative_dates.analysis_entity_id,
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
            tbl_location_types.description AS age_location_desc,
            tbl_dating_uncertainty.uncertainty AS dating_uncertainty,
            relative_date_method.method_name AS relative_date_method_name
            FROM public.tbl_relative_dates
            LEFT JOIN tbl_relative_ages ON tbl_relative_ages.relative_age_id = tbl_relative_dates.relative_age_id
            LEFT JOIN tbl_relative_age_types ON tbl_relative_age_types.relative_age_type_id = tbl_relative_ages.relative_age_type_id
            LEFT JOIN tbl_locations ON tbl_locations.location_id = tbl_relative_ages.location_id
            LEFT JOIN tbl_location_types ON tbl_location_types.location_type_id = tbl_locations.location_type_id
            LEFT JOIN tbl_dating_uncertainty ON tbl_dating_uncertainty.dating_uncertainty_id = tbl_relative_dates.dating_uncertainty_id
            LEFT JOIN tbl_methods AS relative_date_method ON relative_date_method.method_id = tbl_relative_dates.method_id
            WHERE tbl_relative_dates.analysis_entity_id = ANY($1::bigint[])
            ORDER BY tbl_relative_dates.analysis_entity_id, tbl_relative_dates.relative_date_id;
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
            WHERE tbl_analysis_entities.analysis_entity_id = ANY($1::bigint[]);
            `;

            let entityAgesSql = `
            SELECT * FROM tbl_analysis_entity_ages
            WHERE analysis_entity_id = ANY($1::bigint[])`;

            //An analysis entity is dated either by a row in tbl_geochronology (radiometric and similar ages) or
            //by a row in tbl_relative_dates, never both. Which one is decided by the data rather than by the
            //dataset's method, since many methods (e.g. U-series, humic C14, TL) have geochronology rows.
            const allEntities = [];
            site.sample_groups.forEach(sampleGroup => {
                sampleGroup.physical_samples.forEach(physicalSample => {
                    physicalSample.analysis_entities.forEach(analysisEntity => {
                        allEntities.push(analysisEntity);
                    });
                })
            });

            const c14Rows = allEntities.length > 0 ? await pgClient.query(c14stdSql,
                [allEntities.map(analysisEntity => analysisEntity.analysis_entity_id)]) : { rows: [] };
            const c14ByEntity = new Map(c14Rows.rows.map(row => [String(row.analysis_entity_id), row]));

            const relativeDateEntities = [];
            const c14StdEntities = [];
            allEntities.forEach(analysisEntity => {
                if(c14ByEntity.has(String(analysisEntity.analysis_entity_id))) {
                    c14StdEntities.push(analysisEntity);
                }
                else {
                    relativeDateEntities.push(analysisEntity);
                }
            });

            if(relativeDateEntities.length > 0) {
                const relativeDates = await pgClient.query(sql,
                    [relativeDateEntities.map(analysisEntity => analysisEntity.analysis_entity_id)]);
                //Only the first row per entity was ever used. Ordering by
                //relative_date_id makes which one that is deterministic; eight
                //analysis entities in the database have more than one.
                const datingValuesByEntity = new Map();
                relativeDates.rows.forEach(row => {
                    const key = String(row.analysis_entity_id);
                    if(!datingValuesByEntity.has(key)) {
                        //analysis_entity_id is selected only to group the rows;
                        //the per-entity query it replaces did not select it.
                        delete row.analysis_entity_id;
                        datingValuesByEntity.set(key, row);
                    }
                });
                relativeDateEntities.forEach(analysisEntity => {
                    analysisEntity.dating_values = datingValuesByEntity.get(String(analysisEntity.analysis_entity_id));
                });
            }

            if(c14StdEntities.length > 0) {
                const datingLabIds = [];
                c14StdEntities.forEach(analysisEntity => {
                    const r = c14ByEntity.get(String(analysisEntity.analysis_entity_id));
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

                    const datingLabId = parseInt(r.dating_lab_id);
                    if(!Number.isNaN(datingLabId) && !datingLabIds.includes(datingLabId)) {
                        datingLabIds.push(datingLabId);
                    }
                });

                await this.fetchDatingLabs(site, pgClient, datingLabIds);
            }

            if(allEntities.length > 0) {
                const entityAges = await pgClient.query(entityAgesSql,
                    [allEntities.map(analysisEntity => analysisEntity.analysis_entity_id)]);
                const agesByEntity = new Map(entityAges.rows.map(row => [String(row.analysis_entity_id), row]));
                allEntities.forEach(analysisEntity => {
                    analysisEntity.entity_ages = agesByEntity.get(String(analysisEntity.analysis_entity_id));
                });
            }
        }
        finally {
            await this.app.releaseDbConnection(pgClient);
        }

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

    /**
     * Resolves a set of dating labs into the site's lab lookup.
     *
     * Replaces fetchDatingLab(), which took its own pooled connection and ran a
     * query for every C14 analysis entity of the site.
     */
    async fetchDatingLabs(site, pgClient, datingLabIds) {
        if(datingLabIds.length == 0) {
            return;
        }

        if(typeof site.lookup_tables.labs == "undefined") {
            site.lookup_tables.labs = [];
        }

        const values = await pgClient.query(
            "SELECT * FROM tbl_dating_labs WHERE dating_lab_id = ANY($1::int[])", [datingLabIds]);
        const labsById = new Map(values.rows.map(lab => [String(lab.dating_lab_id), lab]));

        datingLabIds.forEach(datingLabId => {
            const alreadyPresent = site.lookup_tables.labs.some(lab => lab && lab.dating_lab_id == datingLabId);
            if(!alreadyPresent) {
                site.lookup_tables.labs.push(labsById.get(String(datingLabId)));
            }
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

                dataGroup.biblio_ids = Array.from(biblioIds);

                dataGroups.push(dataGroup);
            }  
        }

        return site.data_groups = dataGroups.concat(site.data_groups);
    }

    /**
     * Compiles site.age_summary - every dated analysis entity of the site normalized to years BP (before 1950,
     * larger is older) and grouped by dating method, so the extent of each dating method can be compared.
     *
     * Sources: tbl_geochronology and tbl_relative_dates (via dating_values), tbl_analysis_entity_ages (via
     * entity_ages) and dendrochronology data groups. Must run after every module's postProcessSiteData, since
     * the dendro data groups are created there.
     *
     * Values are converted, never reinterpreted: conventional radiocarbon ages stay radiocarbon years and are
     * flagged as such (radiocarbon_years), and a missing bound is flagged as open rather than guessed.
     */
    compileAgeSummary(site) {
        //The analysis entities are read from the datasets, since they have been unlinked from the samples by now
        const ages = [];
        site.datasets.forEach(dataset => {
            (dataset.analysis_entities || []).forEach(analysisEntity => {
                let age = null;
                if(analysisEntity.entity_ages) {
                    age = this.normalizeEntityAge(analysisEntity.entity_ages);
                }
                else if(analysisEntity.dating_values && typeof analysisEntity.dating_values.geochron_id != "undefined") {
                    age = this.normalizeGeochronologyAge(analysisEntity.dating_values, dataset.method_id);
                }
                else if(analysisEntity.dating_values) {
                    age = this.normalizeRelativeAge(analysisEntity.dating_values);
                }

                if(age) {
                    //A relative date can name its own dating method, e.g. the archaeological period of a ceramics analysis
                    if(age.source == "relative_date" && analysisEntity.dating_values.method_id) {
                        age.method_id = analysisEntity.dating_values.method_id;
                        age.method_name = analysisEntity.dating_values.relative_date_method_name;
                    }
                    else {
                        age.method_id = dataset.method_id;
                    }
                    age.analysis_entity_id = analysisEntity.analysis_entity_id;
                    age.physical_sample_id = analysisEntity.physical_sample_id;
                    ages.push(age);
                }
            });
        });

        this.getDendroAges(site).forEach(age => ages.push(age));

        const datings = new Map();
        ages.forEach(age => {
            if(!datings.has(age.method_id)) {
                const method = this.app.getMethodByMethodId(site, age.method_id);
                let methodName = method ? method.method_name : age.method_name;
                datings.set(age.method_id, {
                    method_id: age.method_id,
                    method_name: methodName ? methodName : "Method "+age.method_id,
                    older: age.older,
                    younger: age.younger,
                    open_older: false,
                    open_younger: false,
                    sample_count: 0,
                    age_count: 0,
                    radiocarbon_years_count: 0,
                    approximate_count: 0,
                    disputed_count: 0,
                    swapped_count: 0,
                    ages: []
                });
            }
            const dating = datings.get(age.method_id);
            dating.older = Math.max(dating.older, age.older);
            dating.younger = Math.min(dating.younger, age.younger);
            dating.age_count++;
            if(age.radiocarbon_years) dating.radiocarbon_years_count++;
            if(age.approximate) dating.approximate_count++;
            if(age.disputed) dating.disputed_count++;
            if(age.swapped) dating.swapped_count++;
            dating.ages.push(age);
        });

        datings.forEach(dating => {
            //A bound of the method's extent is open if an age that sets it is open in that direction
            dating.open_older = dating.ages.some(age => age.older == dating.older && age.open_older);
            dating.open_younger = dating.ages.some(age => age.younger == dating.younger && age.open_younger);
            dating.sample_count = new Set(dating.ages.map(age => age.physical_sample_id)).size;
        });

        const datingsList = Array.from(datings.values()).sort((a, b) => b.older - a.older);

        site.age_summary = {
            bp_year: this.bpYear,
            older: datingsList.length > 0 ? Math.max(...datingsList.map(dating => dating.older)) : null,
            younger: datingsList.length > 0 ? Math.min(...datingsList.map(dating => dating.younger)) : null,
            datings: datingsList
        };

        return site.age_summary;
    }

    /*
    * Function: normalizeGeochronologyAge
    *
    * A tbl_geochronology age with its errors. The dating uncertainty marks open ended ages: '>' and 'To'
    * give the youngest possible age (the sample is older), '<' and 'From' give the oldest possible age.
    */
    normalizeGeochronologyAge(datingValues, methodId) {
        const age = this.parseAgeValue(datingValues.age);
        if(age === null) {
            return null;
        }
        const errorOlder = this.parseAgeValue(datingValues.error_older);
        const errorYounger = this.parseAgeValue(datingValues.error_younger);
        const uncertainty = datingValues.dating_uncertainty;

        return {
            source: "geochronology",
            older: age + (errorOlder !== null ? errorOlder : 0),
            younger: age - (errorYounger !== null ? errorYounger : 0),
            open_older: uncertainty == ">" || uncertainty == "To",
            open_younger: uncertainty == "<" || uncertainty == "From",
            radiocarbon_years: this.radiocarbonYearsMethodIds.includes(methodId),
            approximate: this.isApproximateUncertainty(uncertainty),
            disputed: uncertainty == "?",
            swapped: false,
            uncertainty: uncertainty ? uncertainty : null
        };
    }

    /*
    * Function: normalizeRelativeAge
    *
    * A tbl_relative_ages range. cal_age_* is calendar years BP and is preferred; c14_age_* is radiocarbon
    * years and is only used when there are no calendar values. A range of 0 - 0 is a placeholder for
    * unknown, not the year 1950.
    */
    normalizeRelativeAge(datingValues) {
        let older = this.parseAgeValue(datingValues.cal_age_older);
        let younger = this.parseAgeValue(datingValues.cal_age_younger);
        let radiocarbonYears = false;
        if(this.isEmptyAgeRange(older, younger)) {
            older = this.parseAgeValue(datingValues.c14_age_older);
            younger = this.parseAgeValue(datingValues.c14_age_younger);
            radiocarbonYears = true;
        }
        if(this.isEmptyAgeRange(older, younger)) {
            return null;
        }

        const age = this.makeRange(older, younger, datingValues.relative_age_type_id != this.calendarDateAgeTypeId);
        age.source = "relative_date";
        age.radiocarbon_years = radiocarbonYears;
        age.approximate = this.isApproximateUncertainty(datingValues.dating_uncertainty);
        age.disputed = datingValues.dating_uncertainty == "?";
        age.uncertainty = datingValues.dating_uncertainty ? datingValues.dating_uncertainty : null;
        age.relative_age_name = datingValues.relative_age_name;
        return age;
    }

    /*
    * Function: normalizeEntityAge
    *
    * A tbl_analysis_entity_ages row - a modelled age in calendar years BP (calibrated, also for 14C).
    */
    normalizeEntityAge(entityAge) {
        let older = this.parseAgeValue(entityAge.age_older);
        let younger = this.parseAgeValue(entityAge.age_younger);
        if(older === null && younger === null) {
            older = younger = this.parseAgeValue(entityAge.age);
        }
        if(older === null && younger === null) {
            return null;
        }

        const age = this.makeRange(older, younger, true);
        age.source = "entity_age";
        age.radiocarbon_years = false;
        age.approximate = false;
        age.disputed = false;
        age.uncertainty = null;
        age.dating_specifier = entityAge.dating_specifier;
        return age;
    }

    /*
    * Function: getDendroAges
    *
    * One age per dendro sample, from the oldest germination year to the youngest felling year (AD),
    * converted to BP. The same estimates as the dendro module's own chart, with fallbacks to the other estimate.
    */
    getDendroAges(site) {
        const dl = new DendroLib();
        const dendroDataGroups = site.data_groups.filter(dataGroup => {
            return dataGroup.method_ids && dataGroup.method_ids.includes(this.dendroMethodId);
        });

        const ages = [];
        dl.dataGroupsToSampleDataObjects(dendroDataGroups).forEach(sampleDataObject => {
            let germinationYear = dl.getOldestGerminationYear(sampleDataObject).value;
            if(!germinationYear) {
                germinationYear = dl.getYoungestGerminationYear(sampleDataObject).value;
            }
            let fellingYear = dl.getYoungestFellingYear(sampleDataObject).value;
            if(!fellingYear) {
                fellingYear = dl.getOldestFellingYear(sampleDataObject).value;
            }
            if(!germinationYear && !fellingYear) {
                return;
            }

            const age = this.makeRange(
                germinationYear ? this.bpYear - germinationYear : null,
                fellingYear ? this.bpYear - fellingYear : null,
                true
            );
            age.source = "dendro";
            age.method_id = this.dendroMethodId;
            age.analysis_entity_id = null;
            age.physical_sample_id = sampleDataObject.physical_sample_id;
            age.radiocarbon_years = false;
            age.approximate = false;
            age.disputed = false;
            age.uncertainty = null;
            age.germination_year_ad = germinationYear ? germinationYear : null;
            age.felling_year_ad = fellingYear ? fellingYear : null;
            ages.push(age);
        });
        return ages;
    }

    /*
    * Function: makeRange
    *
    * Builds an older/younger pair where at least one is known. A missing bound is set to the known one and, if
    * missingIsOpen, flagged as open. Ranges stored the wrong way around are swapped and flagged.
    */
    makeRange(older, younger, missingIsOpen) {
        let openOlder = false;
        let openYounger = false;
        if(older === null) {
            older = younger;
            openOlder = missingIsOpen;
        }
        if(younger === null) {
            younger = older;
            openYounger = missingIsOpen;
        }
        let swapped = false;
        if(older < younger) {
            [older, younger] = [younger, older];
            [openOlder, openYounger] = [openYounger, openOlder];
            swapped = true;
        }
        return {
            older: older,
            younger: younger,
            open_older: openOlder,
            open_younger: openYounger,
            swapped: swapped
        };
    }

    parseAgeValue(value) {
        if(value === null || typeof value == "undefined" || value === "") {
            return null;
        }
        const number = parseFloat(value);
        return Number.isFinite(number) ? number : null;
    }

    isEmptyAgeRange(older, younger) {
        return (older === null && younger === null) || (older === 0 && younger === 0);
    }

    isApproximateUncertainty(uncertainty) {
        return uncertainty == "Ca." || uncertainty == "From ca." || uncertainty == "To ca.";
    }

    async fetchSiteTimeData(site) {
        let siteDatingObject = {
            age_older: null,
            older_dataset_id: null,
            older_analysis_entity_id: null,
            older_type: null,
            age_younger: null,
            younger_dataset_id: null,
            younger_analysis_entity_id: null,
            younger_type: null,
            // date_type: null, // Consider if you need separate older/younger type
        };

        let dendroDatings = this.getDendroDatingExtremes(site);

        if(dendroDatings.age_older && (dendroDatings.age_older < siteDatingObject.age_older || siteDatingObject.age_older == null)) {
            siteDatingObject.age_older = dendroDatings.age_older;
            siteDatingObject.older_dataset_id = dendroDatings.older_dataset_id;
            siteDatingObject.older_analysis_entity_id = dendroDatings.older_analysis_entity_id;
            siteDatingObject.older_type = dendroDatings.older_type;
        }
        if(dendroDatings.age_younger && (dendroDatings.age_younger > siteDatingObject.age_younger || siteDatingObject.age_younger == null)) {
            siteDatingObject.age_younger = dendroDatings.age_younger;
            siteDatingObject.younger_dataset_id = dendroDatings.younger_dataset_id;
            siteDatingObject.younger_analysis_entity_id = dendroDatings.younger_analysis_entity_id;
            siteDatingObject.younger_type = dendroDatings.younger_type;
        }

        //TODO: implement the rest of the dating methods here, like C14, etc.
        site.datasets.forEach(dataset => {
            let datingSummary = this.getNormalizedDatingSpanFromDataset(dataset);
            if(datingSummary.dating_range_age_type_id == 1) { //dating_range_age_type_id is an "AD" dating
                if(datingSummary.dating_range_low_value < siteDatingObject.age_older || siteDatingObject.age_older == null) {
                    siteDatingObject.age_older = datingSummary.dating_range_low_value;
                    siteDatingObject.older_dataset_id = datingSummary.dataset_id;
                    siteDatingObject.older_analysis_entity_id = datingSummary.analysis_entity_id;
                    // Assuming datingSummary has older_type
                    siteDatingObject.older_type = datingSummary.older_type;
                }
    
                if(datingSummary.dating_range_high_value > siteDatingObject.age_younger || siteDatingObject.age_younger == null) {
                    siteDatingObject.age_younger = datingSummary.dating_range_high_value;
                    siteDatingObject.younger_dataset_id = datingSummary.dataset_id;
                    siteDatingObject.younger_analysis_entity_id = datingSummary.analysis_entity_id;
                    // Assuming datingSummary has younger_type
                    siteDatingObject.younger_type = datingSummary.younger_type;
                }
            }
            else {
                console.warn("This dataset has a dating range type ("+datingSummary.dating_range_age_type_id+") that is not AD, so it will not be included in the site dating summary");
            }
        });


        return siteDatingObject;
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

    async fetchSiteTimeDataOLD(site) {
        let siteDatingObject = {
            age_older: null,
            older_dataset_id: null,
            older_analysis_entity_id: null,
            older_type: null,
            age_younger: null,
            younger_dataset_id: null,
            younger_analysis_entity_id: null,
            younger_type: null,
            // date_type: null, // Consider if you need separate older/younger type
        };
    
        site.datasets.forEach(dataset => {
            let datingSummary = this.getNormalizedDatingSpanFromDataset(dataset);
            if(datingSummary.dating_range_age_type_id == 1) { //dating_range_age_type_id is an "AD" dating
                if(datingSummary.dating_range_low_value < siteDatingObject.age_older || siteDatingObject.age_older == null) {
                    siteDatingObject.age_older = datingSummary.dating_range_low_value;
                    siteDatingObject.older_dataset_id = datingSummary.dataset_id;
                    siteDatingObject.older_analysis_entity_id = datingSummary.analysis_entity_id;
                    // Assuming datingSummary has older_type
                    siteDatingObject.older_type = datingSummary.older_type;
                }
    
                if(datingSummary.dating_range_high_value > siteDatingObject.age_younger || siteDatingObject.age_younger == null) {
                    siteDatingObject.age_younger = datingSummary.dating_range_high_value;
                    siteDatingObject.younger_dataset_id = datingSummary.dataset_id;
                    siteDatingObject.younger_analysis_entity_id = datingSummary.analysis_entity_id;
                    // Assuming datingSummary has younger_type
                    siteDatingObject.younger_type = datingSummary.younger_type;
                }
            }
            else {
                console.warn("This dataset has a dating range type ("+datingSummary.dating_range_age_type_id+") that is not AD, so it will not be included in the site dating summary");
            }
        });
    
        let dl = new DendroLib();
        site.data_groups.forEach(dataGroup => {
            if(dataGroup.method_ids.includes(10)) { //if this is a dendro data group
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
                    // Potentially fetch and assign older_analysis_entity_id if relevant for dendro
                    siteDatingObject.older_type = "dendro";
                }
                if(youngestFellingYear.value != null && (youngestFellingYear.value > siteDatingObject.age_younger || siteDatingObject.age_younger == null)) {
                    siteDatingObject.age_younger = youngestFellingYear.value;
                    siteDatingObject.younger_dataset_id = dataGroup.dataset_id;
                    // Potentially fetch and assign younger_analysis_entity_id if relevant for dendro
                    siteDatingObject.younger_type = "dendro";
                }
            }
        });
    
        // Consider the logic here: if dendro data is present, it might overwrite
        // the older/younger type even if an earlier AD date was more extreme.
        // You might need a more sophisticated way to determine the overall
        // oldest and youngest and their types.
    
        if(siteDatingObject.older_type == "dendro") { // Check older_type instead of date_type
            const currentYear = new Date().getFullYear();
            const diff = currentYear - 1950;
    
            if (siteDatingObject.age_older !== null) {
                siteDatingObject.age_older = siteDatingObject.age_older - diff;
            }
        }
        if(siteDatingObject.younger_type == "dendro") { // Check younger_type instead of date_type
            const currentYear = new Date().getFullYear();
            const diff = currentYear - 1950;
    
            if (siteDatingObject.age_younger !== null) {
                siteDatingObject.age_younger = siteDatingObject.age_younger - diff;
            }
            //FTURE ME: PLEASE CHECK THAT THIS BP CALC IS CORRECT :D
        }
    
        site.chronology_extremes = siteDatingObject;
        return siteDatingObject;
    }

    

}

export default DatingModule;
