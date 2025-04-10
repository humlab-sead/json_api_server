export default class AdnaModule {
    constructor(app) {
        this.name = "aDNA";
        this.moduleMethods = [175]; //method_id 175 used to be for isotopes/IRMS, but it's (at least temporarily) used for aDNA
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
        
        let dataGroups = await this.getMeasurementsForSite(site.site_id);

        if(!site.data_groups) {
            site.data_groups = [];
        }

        site.data_groups = site.data_groups.concat(dataGroups);

        site.lookup_tables.adna = await this.fetchAdnaLookup();

        return site;
    }

    async getMeasurementsForSite(siteId) {
        let site = {
            siteId: siteId
        };
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sql = `
        SELECT 
        tbl_analysis_values.*,
        tbl_value_classes.*,
        tbl_physical_samples.physical_sample_id,
        tbl_physical_samples.sample_name,
		tbl_physical_samples.date_sampled,
		tbl_analysis_dating_ranges.low_value AS dating_range_low_value,
		tbl_analysis_dating_ranges.high_value AS dating_range_high_value,
		tbl_analysis_dating_ranges.low_is_uncertain AS dating_range_low_is_uncertain,
		tbl_analysis_dating_ranges.high_is_uncertain AS dating_range_high_is_uncertain,
		tbl_analysis_dating_ranges.low_qualifier AS dating_range_low_qualifier,
		tbl_analysis_dating_ranges.age_type_id AS dating_range_age_type_id,
		tbl_analysis_dating_ranges.season_id AS dating_range_season_id,
		tbl_analysis_dating_ranges.dating_uncertainty_id AS dating_range_dating_uncertainty_id,
		tbl_analysis_dating_ranges.is_variant AS dating_range_is_variant,
        tbl_analysis_entities.dataset_id
        FROM
        tbl_analysis_values
        LEFT JOIN tbl_value_classes ON tbl_value_classes.value_class_id=tbl_analysis_values.value_class_id
        JOIN tbl_analysis_entities ON tbl_analysis_entities.analysis_entity_id=tbl_analysis_values.analysis_entity_id
        JOIN tbl_physical_samples ON tbl_physical_samples.physical_sample_id=tbl_analysis_entities.physical_sample_id
        LEFT JOIN tbl_sample_groups sg ON sg.sample_group_id = tbl_physical_samples.sample_group_id
        LEFT JOIN tbl_sites ON tbl_sites.site_id = sg.site_id
		LEFT JOIN tbl_analysis_dating_ranges ON tbl_analysis_dating_ranges.analysis_value_id=tbl_analysis_values.analysis_value_id
        WHERE tbl_sites.site_id=$1
        `;

        //tbl_analysis_dating_ranges

        let data = await pgClient.query(sql, [siteId]);
        site.measurements = data.rows;

        this.app.releaseDbConnection(pgClient);
        return this.dbRowsToDataGroups(site.measurements);
    }

    dbRowsToDataGroups(rows) {
        let dataGroupIdCounter = 1;
    
        const groupedBySampleId = {};
    
        for (const row of rows) {
            const psId = row.physical_sample_id;
    
            if (!groupedBySampleId[psId]) {
                groupedBySampleId[psId] = {
                    data_group_id: dataGroupIdCounter++,
                    physical_sample_id: psId,
                    sample_name: row.sample_name,
                    date_sampled: row.date_sampled,
                    biblio_ids: [],
                    method_ids: [row.method_id || 10], // fallback
                    method_group_ids: [],
                    values: []
                };
            }
    
            const dataGroup = groupedBySampleId[psId];
    
            // Add biblio_ids uniquely if available
            if (row.biblio_ids && Array.isArray(row.biblio_ids)) {
                row.biblio_ids.forEach(id => {
                    if (!dataGroup.biblio_ids.includes(id)) {
                        dataGroup.biblio_ids.push(id);
                    }
                });
            }
    
            const value = {
                analysis_entitity_id: row.analysis_entity_id || null,
                dataset_id: row.dataset_id || null,
                valueClassId: row.value_class_id,
                key: row.name,
                value: row.analysis_value,
                valueType: (row.dating_range_low_value || row.dating_range_high_value) ? "complex" : "simple",
                data: null,
                methodId: row.method_id || 10,
            };
    
            if (value.valueType === "complex") {
                value.data = {
                    dating_range_low_value: row.dating_range_low_value,
                    dating_range_high_value: row.dating_range_high_value,
                    dating_range_low_is_uncertain: row.dating_range_low_is_uncertain,
                    dating_range_high_is_uncertain: row.dating_range_high_is_uncertain,
                    dating_range_low_qualifier: row.dating_range_low_qualifier,
                    dating_range_age_type_id: row.dating_range_age_type_id,
                    dating_range_season_id: row.dating_range_season_id,
                    dating_range_dating_uncertainty_id: row.dating_range_dating_uncertainty_id,
                    dating_range_is_variant: row.dating_range_is_variant
                };
            }
    
            dataGroup.values.push(value);
        }
    
        return Object.values(groupedBySampleId);
    }
    

    async fetchAdnaLookup() {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        if(this.moduleMethods.length > 1) {
            console.warn("AdnaModule: More than one method_id detected. This is not currently supported.");
        }

        let methodId = this.moduleMethods[0]; //Making an assumption here that we will onle ever have just 1 method_id
        let valueClasses = await pgClient.query(`
            SELECT DISTINCT(tbl_value_classes.value_class_id)
            FROM tbl_analysis_values
            LEFT JOIN tbl_value_classes ON tbl_value_classes.value_class_id = tbl_analysis_values.value_class_id
            WHERE tbl_value_classes.method_id = $1
            `, [methodId]);

        let valueClassIds = valueClasses.rows.map(row => row.value_class_id);

        let data = await pgClient.query(`
            SELECT DISTINCT ON (tbl_analysis_values.value_class_id)
                tbl_analysis_values.value_class_id,
                tbl_value_classes.value_type_id,
                tbl_value_classes.method_id,
                tbl_value_classes.parent_id,
                tbl_value_classes.name,
                tbl_value_classes.description
            FROM tbl_analysis_values
            LEFT JOIN tbl_value_classes 
                ON tbl_value_classes.value_class_id = tbl_analysis_values.value_class_id
            WHERE tbl_value_classes.value_class_id = ANY($1::int[])
            ORDER BY tbl_analysis_values.value_class_id, tbl_value_classes.name;
            `, [valueClassIds]);

        this.app.releaseDbConnection(pgClient);
        return data.rows;
    }

    datasetHasModuleMethods(dataset) {
        return this.moduleMethods.includes(dataset.method_id);
    }

    postProcessSiteData(site) {
        let dataGroups = [];

        return site.data_groups = dataGroups.concat(site.data_groups);
    }
}
