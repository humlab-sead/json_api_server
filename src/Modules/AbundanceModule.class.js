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
        for(let key in site.analysis_methods) {
            if(this.moduleMethods.includes(site.analysis_methods[key].method_id)) {
                return true;
            }
        }
        return false;
    }

    datasetHasModuleMethods(dataset) {
        return this.moduleMethods.includes(dataset.method_id);
    }

    async fetchSiteData(site) {
        if(!this.siteHasModuleMethods(site)) {
            //console.log("No abundance methods for site "+site.site_id);
            return site;
        }

        //console.log("Fetching abundance data for site "+site.site_id);

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }
        
        let queryPromises = [];

        /*
        site.datasets.forEach(dataset => {
            dataset.analysis_entities.forEach(ae => {

            });
        });
        */

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
                            sql = `
                            SELECT
                            tbl_abundance_elements.element_name,
                            tbl_abundance_elements.element_description,
                            tbl_record_types.record_type_name,
                            tbl_record_types.record_type_description,
                            FROM tbl_abundance_elements 
                            LEFT JOIN tbl_record_types ON tbl_abundance_elements.record_type_id = tbl_record_types.record_type_id
                            WHERE abundance_element_id=$1
                            `;
                            await pgClient.query('SELECT * FROM tbl_abundance_elements WHERE abundance_element_id=$1', [abundance.abundance_element_id]).then(abundanceElements => {
                                abundance.elements = abundanceElements.rows;
                            });

                            //Fetch abundance modifications
                            sql = `
                            SELECT
                            tbl_abundance_modifications.modification_type_id,
                            tbl_modification_types.modification_type_name,
                            tbl_modification_types.modification_type_description
                            FROM tbl_abundance_modifications
                            LEFT JOIN tbl_modification_types ON tbl_abundance_modifications.modification_type_id = tbl_modification_types.modification_type_id
                            WHERE
                            abundance_id=$1
                            `;
                            await pgClient.query(sql, [abundance.abundance_id]).then(abundanceModifications => {
                                abundance.modifications = abundanceModifications.rows;
                            });

                            //Fetch taxon data
                            await pgClient.query('SELECT taxon_id,author_id,genus_id,species FROM tbl_taxa_tree_master WHERE taxon_id=$1', [abundance.taxon_id]).then(async taxon => {
                                abundance.taxon = taxon.rows[0];

                                let family_id = null;
                                if(abundance.taxon.genus_id) {
                                    sql = `SELECT family_id, genus_name FROM tbl_taxa_tree_genera WHERE genus_id=$1`;
                                    await pgClient.query(sql, [abundance.taxon.genus_id]).then(genus => {
                                        family_id = genus.rows[0].family_id;
                                        abundance.taxon.genus = {
                                            genus_id: abundance.taxon.genus_id,
                                            genus_name: genus.rows[0].genus_name
                                        };
                                    });
                                }
                                
                                let order_id = null;
                                if(family_id) {
                                    sql = `SELECT family_name, order_id FROM tbl_taxa_tree_families WHERE family_id=$1`;
                                    await pgClient.query(sql, [family_id]).then(fam => {
                                        order_id = fam.rows[0].order_id;
                                        abundance.taxon.family = {
                                            family_id: family_id,
                                            family_name: fam.rows[0].family_name
                                        };
                                    });
                                }
                                
                                if(order_id) {
                                    sql = `SELECT order_name, record_type_id FROM tbl_taxa_tree_orders WHERE order_id=$1`;
                                    await pgClient.query(sql, [order_id]).then(order => {
                                        abundance.taxon.order = {
                                            order_id: order_id,
                                            order_name: order.rows[0].order_name,
                                            record_type_id: order.rows[0].record_type_id
                                        };
                                    });
                                }
                                

                                sql = `
                                SELECT *
                                FROM tbl_taxa_common_names
                                LEFT JOIN tbl_languages ON tbl_taxa_common_names.language_id = tbl_languages.language_id
                                WHERE taxon_id=$1
                                `;
                                await pgClient.query(sql, [abundance.taxon_id]).then(commonNames => {
                                    abundance.taxon.common_names = commonNames.rows;
                                });

                                await pgClient.query('SELECT measured_attribute_id,attribute_measure,attribute_type,attribute_units,data FROM tbl_taxa_measured_attributes WHERE taxon_id=$1', [abundance.taxon_id]).then(measuredAttr => {
                                    abundance.taxon.measured_attributes = measuredAttr.rows;
                                });

                                await pgClient.query('SELECT * FROM tbl_taxonomy_notes WHERE taxon_id=$1', [abundance.taxon_id]).then(taxNotes => {
                                    abundance.taxon.taxonomy_notes = taxNotes.rows;
                                });

                                await pgClient.query('SELECT * FROM tbl_text_biology WHERE taxon_id=$1', [abundance.taxon_id]).then(textBio => {
                                    abundance.taxon.text_biology = textBio.rows;
                                });
                                
                                await pgClient.query('SELECT * FROM tbl_text_distribution WHERE taxon_id=$1', [abundance.taxon_id]).then(textDist => {
                                    abundance.taxon.text_distribution = textDist.rows;
                                });

                                sql = `
                                SELECT * FROM tbl_ecocodes
                                LEFT JOIN tbl_ecocode_definitions ON tbl_ecocodes.ecocode_definition_id = tbl_ecocode_definitions.ecocode_definition_id
                                WHERE tbl_ecocodes.taxon_id=$1
                                `;
                                await pgClient.query(sql, [abundance.taxon_id]).then(ecoCodes => {
                                    abundance.taxon.ecocodes = ecoCodes.rows;
                                });

                                sql = `
                                SELECT * FROM tbl_taxa_seasonality
                                LEFT JOIN tbl_seasons ON tbl_taxa_seasonality.season_id = tbl_seasons.season_id
                                LEFT JOIN tbl_activity_types ON tbl_taxa_seasonality.activity_type_id = tbl_activity_types.activity_type_id
                                WHERE tbl_taxa_seasonality.taxon_id=$1
                                `;
                                await pgClient.query(sql, [abundance.taxon_id]).then(seasonality => {
                                    abundance.taxon.seasonality = seasonality.rows;
                                });
                                
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
    
    postProcessSiteData(site) {
    
        let dataGroups = [];

        for(let dsKey in site.datasets) {
            let dataset = site.datasets[dsKey];
            if(this.datasetHasModuleMethods(dataset)) {

                let method = this.app.getAnalysisMethodByMethodId(site, dataset.method_id);
                
                let dataGroup = {
                    id: dataset.dataset_id,
                    dataset_name: dataset.dataset_name,
                    method_id: dataset.method_id,
                    method_group_id: dataset.method_group_id,
                    method_name: method.method_name,
                    type: "abundance",
                    data_points: []
                }

                for(let aeKey in dataset.analysis_entities) {
                    let ae = dataset.analysis_entities[aeKey];
                    if(ae.dataset_id == dataGroup.id) {
                        if(ae.abundances) {
                            for(let abundanceKey in ae.abundances) {
                                let abundance = ae.abundances[abundanceKey];

                                let sampleName = this.app.getSampleNameBySampleId(site, ae.physical_sample_id);

                                dataGroup.data_points.push({
                                    physical_sample_id: ae.physical_sample_id,
                                    sample_name: sampleName,
                                    label: ae.physical_sample_id,
                                    value: abundance
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

module.exports = AbundanceModule;