class AbundanceModule {
    constructor(app) {
        this.name = "Abundance";
        this.moduleMethods = [3, 6, 8, 15, 40, 111]; //include 60, 81?
        this.app = app;
        this.expressApp = this.app.expressApp;
        this.setupEndpoints();

        /*
        this.getEcocodesFromTaxa([{
            taxon_id: 33588,
            count: 100
        }]);
        */
    }

    setupEndpoints() {
    }

    async getEcocodesFromTaxa(taxa) {
        /*taxa should be an array of objects, like so:
        taxa = [{
                taxon_id: int,
                count: int
            },]
        */
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }
        try {
            let sql = `SELECT mcr_data FROM tbl_mcrdata_birmbeetledat WHERE taxon_id=$1 ORDER BY mcr_row ASC`;

            let queryPromises = [];
            for(let key in taxa) {
                let queryPromise = pgClient.query(sql, [taxa[key].taxon_id]).then(result => {
                    //taxa[key].mcrMatrix = result.rows;

                    let matrix = [];
                    result.rows.forEach(row => {
                        let matrixRow = row.mcr_data;
                        matrix.push(matrixRow);
                    });
                    taxa[key].mcrMatrix = matrix;
                });

                queryPromises.push(queryPromise);
            }

            await Promise.all(queryPromises);
        }
        finally {
            await this.app.releaseDbConnection(pgClient);
        }
    }

    siteHasModuleMethods(site) {
        for(let key in site.lookup_tables.methods) {
            if(this.moduleMethods.includes(site.lookup_tables.methods[key].method_id)) {
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

    /**
     * Collects every analysis entity of the site, in the order they appear under
     * the sample groups.
     */
    getSiteAnalysisEntities(site) {
        const analysisEntities = [];
        site.sample_groups.forEach(sampleGroup => {
            sampleGroup.physical_samples.forEach(physicalSample => {
                physicalSample.analysis_entities.forEach(analysisEntity => {
                    analysisEntities.push(analysisEntity);
                });
            });
        });
        return analysisEntities;
    }

    /**
     * Fetches all abundance data for a site.
     *
     * This used to run one query per analysis entity, then a further ~4 queries
     * per abundance row and ~13 more per row to resolve its taxon, with no
     * deduplication: a site with 5573 abundances issued tens of thousands of
     * queries. It now issues a fixed number of set-based queries — the abundance
     * rows and their satellites for the whole site, then the distinct taxa — and
     * assembles the same structure in JS.
     */
    async fetchSiteData(site, verbose = false) {
        if(!this.siteHasModuleMethods(site)) {
            //console.log("No abundance methods for site "+site.site_id);
            return site;
        }

        if(verbose) {
            console.log("Fetching abundance data for site "+site.site_id);
        }

        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        try {
            const analysisEntities = this.getSiteAnalysisEntities(site);
            //Every analysis entity of the site gets an abundances array, whether
            //or not it has any rows, as the per-entity query it replaces did.
            analysisEntities.forEach(analysisEntity => {
                analysisEntity.abundances = [];
            });

            if(analysisEntities.length == 0) {
                return site;
            }

            const analysisEntityIds = analysisEntities.map(analysisEntity => analysisEntity.analysis_entity_id);
            const analysisEntitiesById = new Map(
                analysisEntities.map(analysisEntity => [String(analysisEntity.analysis_entity_id), analysisEntity]));

            const abundanceResult = await pgClient.query(
                'SELECT * FROM tbl_abundances WHERE analysis_entity_id = ANY($1::bigint[]) ORDER BY analysis_entity_id, abundance_id',
                [analysisEntityIds]);
            const abundances = abundanceResult.rows;

            abundances.forEach(abundance => {
                abundance.identification_levels = [];
                abundance.modifications = [];
                const analysisEntity = analysisEntitiesById.get(String(abundance.analysis_entity_id));
                if(analysisEntity) {
                    analysisEntity.abundances.push(abundance);
                }
            });

            if(abundances.length == 0) {
                return site;
            }

            const abundanceIds = abundances.map(abundance => abundance.abundance_id);
            const abundancesById = new Map(abundances.map(abundance => [String(abundance.abundance_id), abundance]));

            const identLevels = await pgClient.query(`
                SELECT
                tbl_abundance_ident_levels.abundance_id,
                tbl_abundance_ident_levels.identification_level_id,
                tbl_identification_levels.identification_level_abbrev,
                tbl_identification_levels.identification_level_name,
                tbl_identification_levels.notes
                FROM tbl_abundance_ident_levels
                LEFT JOIN tbl_identification_levels ON tbl_abundance_ident_levels.identification_level_id = tbl_identification_levels.identification_level_id
                WHERE abundance_id = ANY($1::bigint[])
                ORDER BY abundance_id, tbl_abundance_ident_levels.identification_level_id
                `, [abundanceIds]);
            identLevels.rows.forEach(identLevel => {
                const abundance = abundancesById.get(String(identLevel.abundance_id));
                if(abundance) {
                    abundance.identification_levels.push(identLevel);
                }
            });

            const modifications = await pgClient.query(
                'SELECT * FROM tbl_abundance_modifications WHERE abundance_id = ANY($1::bigint[]) ORDER BY abundance_id, modification_type_id',
                [abundanceIds]);
            modifications.rows.forEach(modification => {
                const abundance = abundancesById.get(String(modification.abundance_id));
                if(abundance) {
                    abundance.modifications.push(modification);
                }
            });

            const abundanceElementIds = [];
            abundances.forEach(abundance => {
                if(abundance.abundance_element_id != null && !abundanceElementIds.includes(abundance.abundance_element_id)) {
                    abundanceElementIds.push(abundance.abundance_element_id);
                }
            });
            if(abundanceElementIds.length > 0) {
                const abundanceElements = await pgClient.query(
                    'SELECT * FROM tbl_abundance_elements WHERE abundance_element_id = ANY($1::int[])',
                    [abundanceElementIds]);
                const elementsById = new Map(
                    abundanceElements.rows.map(element => [String(element.abundance_element_id), element]));
                abundanceElementIds.forEach(elementId => {
                    const element = elementsById.get(String(elementId));
                    if(element) {
                        this.addAbundanceElementToLocalLookup(site, element);
                    }
                });
            }

            const modificationTypeIds = [];
            modifications.rows.forEach(modification => {
                if(modification.modification_type_id != null && !modificationTypeIds.includes(modification.modification_type_id)) {
                    modificationTypeIds.push(modification.modification_type_id);
                }
            });
            if(modificationTypeIds.length > 0) {
                const modificationTypes = await pgClient.query(
                    'SELECT * FROM tbl_modification_types WHERE modification_type_id = ANY($1::int[])',
                    [modificationTypeIds]);
                const typesById = new Map(
                    modificationTypes.rows.map(type => [String(type.modification_type_id), type]));
                modificationTypeIds.forEach(typeId => {
                    const type = typesById.get(String(typeId));
                    if(type) {
                        this.addAbundanceModificationTypeToLocalLookup(site, type);
                    }
                });
            }

            const taxonIds = [];
            abundances.forEach(abundance => {
                if(abundance.taxon_id != null
                    && !taxonIds.includes(abundance.taxon_id)
                    && this.getTaxonFromLocalLookup(site, abundance.taxon_id) == null) {
                    taxonIds.push(abundance.taxon_id);
                }
            });
            await this.fetchTaxaForLookup(pgClient, site, taxonIds);
        }
        finally {
            await this.app.releaseDbConnection(pgClient);
        }

        return site;
    }

    /**
     * Resolves a set of taxa and adds them to the site's taxa lookup.
     *
     * Each taxon used to cost ~13 queries and was resolved once per abundance
     * row referencing it. The whole set is now resolved in a fixed number of
     * queries, one per related table, and assembled in JS. The resulting taxon
     * objects are shaped exactly as before, including the quirk that author_id
     * is removed only when the taxon actually has an author.
     */
    async fetchTaxaForLookup(pgClient, site, taxonIds) {
        if(taxonIds.length == 0) {
            return;
        }

        const masterResult = await pgClient.query(
            'SELECT taxon_id,author_id,genus_id,species FROM tbl_taxa_tree_master WHERE taxon_id = ANY($1::int[])',
            [taxonIds]);
        const taxaById = new Map(masterResult.rows.map(taxon => [String(taxon.taxon_id), taxon]));

        const genusIds = [];
        masterResult.rows.forEach(taxon => {
            if(taxon.genus_id != null && !genusIds.includes(taxon.genus_id)) {
                genusIds.push(taxon.genus_id);
            }
        });

        let generaById = new Map();
        if(genusIds.length > 0) {
            const genera = await pgClient.query(
                'SELECT genus_id, family_id, genus_name FROM tbl_taxa_tree_genera WHERE genus_id = ANY($1::int[])',
                [genusIds]);
            generaById = new Map(genera.rows.map(genus => [String(genus.genus_id), genus]));
        }

        const familyIds = [];
        generaById.forEach(genus => {
            if(genus.family_id != null && !familyIds.includes(genus.family_id)) {
                familyIds.push(genus.family_id);
            }
        });

        let familiesById = new Map();
        if(familyIds.length > 0) {
            const families = await pgClient.query(
                'SELECT family_id, family_name, order_id FROM tbl_taxa_tree_families WHERE family_id = ANY($1::int[])',
                [familyIds]);
            familiesById = new Map(families.rows.map(family => [String(family.family_id), family]));
        }

        const orderIds = [];
        familiesById.forEach(family => {
            if(family.order_id != null && !orderIds.includes(family.order_id)) {
                orderIds.push(family.order_id);
            }
        });

        let ordersById = new Map();
        if(orderIds.length > 0) {
            const orders = await pgClient.query(
                'SELECT order_id, order_name, record_type_id FROM tbl_taxa_tree_orders WHERE order_id = ANY($1::int[])',
                [orderIds]);
            ordersById = new Map(orders.rows.map(order => [String(order.order_id), order]));
        }

        const authorIds = [];
        masterResult.rows.forEach(taxon => {
            if(taxon.author_id != null && !authorIds.includes(taxon.author_id)) {
                authorIds.push(taxon.author_id);
            }
        });

        let authorsById = new Map();
        if(authorIds.length > 0) {
            const authors = await pgClient.query(
                'SELECT * FROM tbl_taxa_tree_authors WHERE author_id = ANY($1::int[])', [authorIds]);
            authorsById = new Map(authors.rows.map(author => [String(author.author_id), author]));
        }

        const foundTaxonIds = masterResult.rows.map(taxon => taxon.taxon_id);

        /** Runs one query for the whole taxon set and groups the rows by taxon_id. */
        const groupedByTaxon = async (sql) => {
            const grouped = new Map();
            if(foundTaxonIds.length == 0) {
                return grouped;
            }
            const result = await pgClient.query(sql, [foundTaxonIds]);
            result.rows.forEach(row => {
                const key = String(row.taxon_id);
                if(!grouped.has(key)) {
                    grouped.set(key, []);
                }
                grouped.get(key).push(row);
            });
            return grouped;
        };

        const commonNamesByTaxon = await groupedByTaxon(`
            SELECT *
            FROM tbl_taxa_common_names
            LEFT JOIN tbl_languages ON tbl_taxa_common_names.language_id = tbl_languages.language_id
            WHERE taxon_id = ANY($1::int[])
            ORDER BY taxon_id, taxon_common_name_id
            `);
        //taxon_id is selected only to group the rows and is stripped again, since
        //the per-taxon query this replaces did not select it.
        const measuredAttributesByTaxon = await groupedByTaxon(`
            SELECT taxon_id,measured_attribute_id,attribute_measure,attribute_type,attribute_units,data
            FROM tbl_taxa_measured_attributes
            WHERE taxon_id = ANY($1::int[])
            ORDER BY taxon_id, measured_attribute_id
            `);
        measuredAttributesByTaxon.forEach(rows => {
            rows.forEach(row => delete row.taxon_id);
        });
        const taxonomyNotesByTaxon = await groupedByTaxon(`
            SELECT * FROM tbl_taxonomy_notes WHERE taxon_id = ANY($1::int[])
            ORDER BY taxon_id, taxonomy_notes_id
            `);
        const textBiologyByTaxon = await groupedByTaxon(`
            SELECT * FROM tbl_text_biology WHERE taxon_id = ANY($1::int[])
            ORDER BY taxon_id, biology_id
            `);
        const textDistributionByTaxon = await groupedByTaxon(`
            SELECT * FROM tbl_text_distribution WHERE taxon_id = ANY($1::int[])
            ORDER BY taxon_id, distribution_id
            `);
        const seasonalityByTaxon = await groupedByTaxon(`
            SELECT * FROM tbl_taxa_seasonality
            LEFT JOIN tbl_seasons ON tbl_taxa_seasonality.season_id = tbl_seasons.season_id
            LEFT JOIN tbl_activity_types ON tbl_taxa_seasonality.activity_type_id = tbl_activity_types.activity_type_id
            WHERE tbl_taxa_seasonality.taxon_id = ANY($1::int[])
            ORDER BY tbl_taxa_seasonality.taxon_id, tbl_taxa_seasonality.seasonality_id
            `);

        taxonIds.forEach(taxonId => {
            const taxon = taxaById.get(String(taxonId));
            if(!taxon) {
                //The per-row implementation dereferenced this unconditionally and
                //would have thrown; there are no such rows, but skipping is safe.
                return;
            }

            const genus = taxon.genus_id ? generaById.get(String(taxon.genus_id)) : null;
            let familyId = null;
            if(genus) {
                familyId = genus.family_id;
                taxon.genus = {
                    genus_id: taxon.genus_id,
                    genus_name: genus.genus_name,
                };
            }

            let orderId = null;
            const family = familyId ? familiesById.get(String(familyId)) : null;
            if(family) {
                orderId = family.order_id;
                taxon.family = {
                    family_id: familyId,
                    family_name: family.family_name,
                };
            }

            const order = orderId ? ordersById.get(String(orderId)) : null;
            if(order) {
                taxon.order = {
                    order_id: orderId,
                    order_name: order.order_name,
                    record_type_id: order.record_type_id,
                };
            }

            if(taxon.author_id) {
                taxon.author = authorsById.get(String(taxon.author_id));
                delete taxon.author_id;
            }

            const key = String(taxonId);
            taxon.common_names = commonNamesByTaxon.get(key) || [];
            taxon.measured_attributes = measuredAttributesByTaxon.get(key) || [];
            taxon.taxonomy_notes = taxonomyNotesByTaxon.get(key) || [];
            taxon.text_biology = textBiologyByTaxon.get(key) || [];
            taxon.text_distribution = textDistributionByTaxon.get(key) || [];
            taxon.seasonality = seasonalityByTaxon.get(key) || [];

            this.addTaxonToLocalLookup(site, taxon);
        });
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
                    biblio_ids: dataset.biblio_id ? [dataset.biblio_id] : [],
                    method_ids: [dataset.method_id],
                    method_group_ids: [dataset.method_group_id],
                    method_group_id: dataset.method_group_id,
                    method_name: method.method_name,
                    type: "abundance",
                    values: []
                }

                for(let aeKey in dataset.analysis_entities) {
                    let ae = dataset.analysis_entities[aeKey];
                    if(ae.dataset_id == dataGroup.id) {
                        if(ae.abundances) {
                            for(let abundanceKey in ae.abundances) {
                                let abundance = ae.abundances[abundanceKey];
                                //abundance.taxon = this.getTaxonFromLocalLookup(site, abundance.taxon_id);
                                let sampleName = this.app.getSampleNameBySampleId(site, ae.physical_sample_id);

                                dataGroup.values.push({
                                    analysis_entity_id: ae.analysis_entity_id,
                                    dataset_id: dataset.dataset_id,
                                    key: ae.physical_sample_id, 
                                    value: abundance,
                                    valueType: 'complex',
                                    data: abundance,
                                    methodId: dataset.method_id,
                                    physical_sample_id: ae.physical_sample_id,
                                    sample_name: sampleName,
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

export default AbundanceModule;
