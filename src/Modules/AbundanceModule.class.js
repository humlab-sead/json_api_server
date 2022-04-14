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

    getTaxonFromLocalLookup(site, taxon_id) {
        if(typeof site.lookup_tables.taxa == "undefined") {
            site.lookup_tables.taxa = [];
        }
        for(let key in site.lookup_tables.taxa) {
            if(site.lookup_tables.taxa[key].taxon_id == taxon_id) {
                return site.lookup_tables.taxa[key];
            }
        }
        return null;
    }

    addTaxonToLocalLookup(site, taxon) {
        if(typeof site.lookup_tables.taxa == "undefined") {
            site.lookup_tables.taxa = [];
        }
        if(this.getTaxonFromLocalLookup(site, taxon.taxon_id) == null) {
            site.lookup_tables.taxa.push(taxon);
        }
    }

    getAbundanceElementFromLocalLookup(site, abundance_element_id) {
        if(typeof site.lookup_tables.abundance_elements == "undefined") {
            site.lookup_tables.abundance_elements = [];
        }
        for(let key in site.lookup_tables.abundance_elements) {
            if(site.lookup_tables.abundance_elements[key].abundance_element_id == abundance_element_id) {
                return site.lookup_tables.abundance_elements[key];
            }
        }
        return null;
    }

    addAbundanceElementToLocalLookup(site, abundance_element) {
        if(typeof site.lookup_tables.abundance_elements == "undefined") {
            site.lookup_tables.abundance_elements = [];
        }
        if(this.getAbundanceElementFromLocalLookup(site, abundance_element.abundance_element_id) == null) {
            site.lookup_tables.abundance_elements.push(abundance_element);
        }
    }

    getAbundanceModificationTypeFromLocalLookup(site, modification_type_id) {
        if(typeof site.lookup_tables.abundance_modifications == "undefined") {
            site.lookup_tables.abundance_modifications = [];
        }
        for(let key in site.lookup_tables.abundance_modifications) {
            if(site.lookup_tables.abundance_modifications[key].modification_type_id == modification_type_id) {
                return site.lookup_tables.abundance_modifications[key];
            }
        }
        return null;
    }

    addAbundanceModificationTypeToLocalLookup(site, abundance_element) {
        if(typeof site.lookup_tables.abundance_modifications == "undefined") {
            site.lookup_tables.abundance_modifications = [];
        }
        //Final check to see that it's really not registered already
        if(this.getAbundanceModificationTypeFromLocalLookup(site, abundance_element.modification_type_id) == null) {
            site.lookup_tables.abundance_modifications.push(abundance_element);
        }
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

                            let abundanceElement = this.getAbundanceElementFromLocalLookup(site, abundance.abundance_element_id);
                            if(abundanceElement == null) {
                                //Fetch abundance elements
                                /*
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
                                */
                                await pgClient.query('SELECT * FROM tbl_abundance_elements WHERE abundance_element_id=$1', [abundance.abundance_element_id]).then(abundanceElements => {
                                    if(abundanceElements.rows.length > 0) {
                                        this.addAbundanceElementToLocalLookup(site, abundanceElements.rows[0]);
                                    }
                                });
                            }

                            sql = "SELECT * FROM tbl_abundance_modifications WHERE abundance_id=$1";
                            let abundanceModifications = await pgClient.query(sql, [abundance.abundance_id]);
                            abundance.modifications = abundanceModifications.rows;

                            for(let key in abundance.modifications) {
                                let am = abundance.modifications[key];
                                let abundanceModification = this.getAbundanceModificationTypeFromLocalLookup(site, am.modification_type_id);
                                if(abundanceModification == null) {
                                    sql = "SELECT * FROM tbl_modification_types WHERE modification_type_id=$1";
                                    let abundanceModificationResult = await pgClient.query(sql, [am.modification_type_id]);
                                    if(abundanceModificationResult.rows.length > 0) {
                                        this.addAbundanceModificationTypeToLocalLookup(site, abundanceModificationResult.rows[0]);
                                    }
                                }
                            }

                            //Fetch taxon data if we don't already have it
                            let taxon = this.getTaxonFromLocalLookup(site, abundance.taxon_id)
                            if(taxon == null) {
                                let taxon_id = abundance.taxon_id;
                                await pgClient.query('SELECT taxon_id,author_id,genus_id,species FROM tbl_taxa_tree_master WHERE taxon_id=$1', [taxon_id]).then(async taxonData => {
                                    
                                    let taxon = taxonData.rows[0];

                                    let family_id = null;
                                    if(taxon.genus_id) {
                                        sql = `SELECT family_id, genus_name FROM tbl_taxa_tree_genera WHERE genus_id=$1`;
                                        await pgClient.query(sql, [taxon.genus_id]).then(genus => {
                                            family_id = genus.rows[0].family_id;
                                            taxon.genus = {
                                                genus_id: taxon.genus_id,
                                                genus_name: genus.rows[0].genus_name
                                            };
                                        });
                                    }
                                    
                                    let order_id = null;
                                    if(family_id) {
                                        sql = `SELECT family_name, order_id FROM tbl_taxa_tree_families WHERE family_id=$1`;
                                        await pgClient.query(sql, [family_id]).then(fam => {
                                            order_id = fam.rows[0].order_id;
                                            taxon.family = {
                                                family_id: family_id,
                                                family_name: fam.rows[0].family_name
                                            };
                                        });
                                    }
                                    
                                    if(order_id) {
                                        sql = `SELECT order_name, record_type_id FROM tbl_taxa_tree_orders WHERE order_id=$1`;
                                        await pgClient.query(sql, [order_id]).then(order => {
                                            taxon.order = {
                                                order_id: order_id,
                                                order_name: order.rows[0].order_name,
                                                record_type_id: order.rows[0].record_type_id
                                            };
                                        });
                                    }
                                    
                                    if(taxon.author_id) {
                                        await pgClient.query('SELECT * FROM tbl_taxa_tree_authors WHERE author_id=$1', [taxon.author_id]).then(taxa_author => {
                                            taxon.author = taxa_author.rows[0];
                                            delete taxon.author_id;
                                        });
                                    }
                                    
                                    sql = `
                                    SELECT *
                                    FROM tbl_taxa_common_names
                                    LEFT JOIN tbl_languages ON tbl_taxa_common_names.language_id = tbl_languages.language_id
                                    WHERE taxon_id=$1
                                    `;
                                    await pgClient.query(sql, [taxon_id]).then(commonNames => {
                                        taxon.common_names = commonNames.rows;
                                    });

                                    await pgClient.query('SELECT measured_attribute_id,attribute_measure,attribute_type,attribute_units,data FROM tbl_taxa_measured_attributes WHERE taxon_id=$1', [abundance.taxon_id]).then(measuredAttr => {
                                        taxon.measured_attributes = measuredAttr.rows;
                                    });

                                    await pgClient.query('SELECT * FROM tbl_taxonomy_notes WHERE taxon_id=$1', [taxon_id]).then(taxNotes => {
                                        taxon.taxonomy_notes = taxNotes.rows;
                                    });

                                    await pgClient.query('SELECT * FROM tbl_text_biology WHERE taxon_id=$1', [taxon_id]).then(textBio => {
                                        taxon.text_biology = textBio.rows;
                                    });
                                    
                                    await pgClient.query('SELECT * FROM tbl_text_distribution WHERE taxon_id=$1', [taxon_id]).then(textDist => {
                                        taxon.text_distribution = textDist.rows;
                                    });

                                    /* disabling ecocodes fetching for now, it's just a lot of unused data atm
                                    sql = `
                                    SELECT * FROM tbl_ecocodes
                                    LEFT JOIN tbl_ecocode_definitions ON tbl_ecocodes.ecocode_definition_id = tbl_ecocode_definitions.ecocode_definition_id
                                    WHERE tbl_ecocodes.taxon_id=$1
                                    `;
                                    await pgClient.query(sql, [taxon_id]).then(ecoCodes => {
                                        taxon.ecocodes = ecoCodes.rows;
                                    });
                                    */

                                    sql = `
                                    SELECT * FROM tbl_taxa_seasonality
                                    LEFT JOIN tbl_seasons ON tbl_taxa_seasonality.season_id = tbl_seasons.season_id
                                    LEFT JOIN tbl_activity_types ON tbl_taxa_seasonality.activity_type_id = tbl_activity_types.activity_type_id
                                    WHERE tbl_taxa_seasonality.taxon_id=$1
                                    `;
                                    await pgClient.query(sql, [taxon_id]).then(seasonality => {
                                        taxon.seasonality = seasonality.rows;
                                    });
                                
                                    this.addTaxonToLocalLookup(site, taxon);
                                });

                            }
                            

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