import { clean, isBlank } from "./SdfCommon.js";

/**
 * Dataset-rooted source layer for the SEAD Data Format.
 *
 * SDF used to read its Tier B observations out of `site.data_groups`, the
 * display-oriented structure the JSON API's data modules assemble. That was a
 * mistake, and a costly one:
 *
 *   - `data_group_id` is not a database id. Four modules set it to
 *     `dataset.dataset_id`, three (dendro, ceramics, aDNA) to a per-request
 *     counter starting at 1. Nothing outside the response can resolve it,
 *     which puts it in direct conflict with D2 ("joins use the database's own
 *     IDs").
 *   - identity is carried inconsistently. DendrochronologyModule hardcodes
 *     `analysis_entity_id: null` on every value even though its own query
 *     selects the id; CeramicsModule puts it on the group and overwrites it
 *     once per analysis entity; only AdnaModule carries it per value. The
 *     result was that every Dendrochronology and Ceramics row exported with a
 *     null primary key, which the importer reads as an INSERT.
 *   - the modules that build data groups are gated on hardcoded method
 *     allowlists (design F2), so a data group only exists where some module
 *     decided to make one.
 *
 * The data-group concept is a legacy workaround from before the value-class
 * system, when dendro and ceramics had to be modelled one-dataset-per-analysis
 * -entity. It is deprecated. This module reads the real structural model
 * instead — site → sample group → physical sample → analysis entity → dataset —
 * and hangs every observation off the analysis entity that owns it.
 *
 * Nothing here depends on a method allowlist. Scope comes from the site's own
 * sample groups, so a method nobody has written a module for still exports.
 */

//Which value table an analysis entity's observations live in decides which
//sheet it lands on. Routing is by what the database actually holds, never by a
//method id, so a new method needs no code change to be exported.
export const OBSERVATION_SHEETS = {
    ABUNDANCES: "Abundances",
    MEASUREMENTS: "Measurements",
    CERAMICS: "Ceramics",
    ISOTOPES: "Isotopes",
    DENDROCHRONOLOGY: "Dendrochronology",
    ANALYSIS_VALUES: "Analysis Values",
};

//D11: the value-class system is authoritative over the legacy dendro tables,
//so method 10's value classes drive the Dendrochronology sheet.
export const DENDRO_METHOD_ID = 10;

/**
 * base_type on tbl_value_types names the typed subtable a value should live
 * in — but only as an intention. Measured against sead_staging, 777 values
 * whose class is integer-typed actually store their value in
 * tbl_analysis_boolean_values, and 21,300 of 72,183 analysis values have no
 * typed row at all. So every subtable is left-joined and the first one that
 * actually produced a row wins, in this order; `analysis_value` is the
 * fallback. Precedence matters for int4range, where 442 values carry both a
 * dating range and a note.
 */
const TYPED_PRECEDENCE = [
    "integer", "boolean", "categorical", "numerical", "identifier", "dating_range", "note",
];

export default class SdfSource {
    constructor(app) {
        this.app = app;
    }

    async _q(sql, params, label) {
        try {
            const r = await this.app.query(sql, params);
            return r.rows;
        } catch (e) {
            console.warn(`SDF source: query '${label}' failed: ${e.message}`);
            return [];
        }
    }

    /**
     * Loads everything the Tier B sheets need for a set of sites.
     *
     * @param {number[]} siteIds
     * @returns {Promise<object>} the source model
     */
    async load(siteIds) {
        const ids = siteIds.map(v => parseInt(v)).filter(Number.isInteger);
        if (!ids.length) return this._empty();

        const entities = await this._loadEntities(ids);
        const aeIds = entities.map(e => e.analysis_entity_id);

        if (!aeIds.length) {
            return { ...this._empty(), entities: [], entityById: new Map() };
        }

        const [abundances, modifications, measured, ceramics, isotopes, analysisValues] =
            await Promise.all([
                this._loadAbundances(aeIds),
                this._loadAbundanceModifications(aeIds),
                this._loadMeasuredValues(aeIds),
                this._loadCeramics(aeIds),
                this._loadIsotopes(aeIds),
                this._loadAnalysisValues(aeIds),
            ]);

        //Taxa are resolved from the database rather than from the site
        //document's lookup_tables, because the abundances above are no longer
        //constrained to what the API chose to fetch.
        const taxa = await this._loadTaxa([...new Set(
            abundances.map(a => a.taxon_id).filter(v => !isBlank(v)).map(v => parseInt(v)))]);

        //attach modifications to their abundance
        const modsByAbundance = new Map();
        for (const m of modifications) {
            const k = String(m.abundance_id);
            if (!modsByAbundance.has(k)) modsByAbundance.set(k, []);
            modsByAbundance.get(k).push(m.modification_type_name);
        }
        for (const a of abundances) {
            a.modifications = modsByAbundance.get(String(a.abundance_id)) || [];
        }

        const source = {
            siteIds: ids,
            entities,
            entityById: new Map(entities.map(e => [String(e.analysis_entity_id), e])),
            aeIds,
            abundances,
            measured,
            ceramics,
            isotopes,
            analysisValues,
            taxa,
        };

        source.routing = this._route(source);
        return source;
    }

    _empty() {
        return {
            siteIds: [], entities: [], entityById: new Map(), aeIds: [],
            abundances: [], measured: [], ceramics: [], isotopes: [], analysisValues: [],
            taxa: new Map(),
            routing: { byEntity: new Map(), unclassified: [] },
        };
    }

    /** taxon_id -> { taxon_id, genus, family, order, species, label } */
    async _loadTaxa(taxonIds) {
        if (!taxonIds.length) return new Map();
        const rows = await this._q(
            `SELECT t.taxon_id, t.species, g.genus_name, f.family_name, o.order_name
               FROM tbl_taxa_tree_master t
               LEFT JOIN tbl_taxa_tree_genera g ON g.genus_id = t.genus_id
               LEFT JOIN tbl_taxa_tree_families f ON f.family_id = g.family_id
               LEFT JOIN tbl_taxa_tree_orders o ON o.order_id = f.order_id
              WHERE t.taxon_id = ANY($1)`,
            [taxonIds], "taxa");

        return new Map(rows.map(r => [String(r.taxon_id), {
            taxon_id: parseInt(r.taxon_id),
            genus: clean(r.genus_name),
            family: clean(r.family_name),
            order: clean(r.order_name),
            species: clean(r.species),
            label: [clean(r.genus_name), clean(r.species)].filter(Boolean).join(" ")
                || clean(r.family_name) || String(r.taxon_id),
        }]));
    }

    /**
     * The spine: every analysis entity reachable from these sites, with the
     * dataset that owns it and the sample it was taken from.
     *
     * This is the query that replaces `site.data_groups`. It starts at
     * tbl_sample_groups rather than at tbl_datasets because a dataset reaches a
     * site only through its analysis entities — there is no site_id on
     * tbl_datasets.
     */
    async _loadEntities(siteIds) {
        return this._q(
            `SELECT ae.analysis_entity_id,
                    ae.physical_sample_id,
                    ae.dataset_id,
                    ae.date_updated,
                    sg.site_id,
                    sg.sample_group_id,
                    ps.sample_name,
                    d.dataset_name,
                    d.method_id,
                    d.data_type_id,
                    d.biblio_id,
                    d.master_set_id,
                    d.project_id,
                    d.dataset_uuid,
                    m.method_name,
                    m.method_abbrev_or_alt_name,
                    dt.data_type_name
               FROM tbl_sample_groups sg
               JOIN tbl_physical_samples ps ON ps.sample_group_id = sg.sample_group_id
               JOIN tbl_analysis_entities ae ON ae.physical_sample_id = ps.physical_sample_id
               LEFT JOIN tbl_datasets d ON d.dataset_id = ae.dataset_id
               LEFT JOIN tbl_methods m ON m.method_id = d.method_id
               LEFT JOIN tbl_data_types dt ON dt.data_type_id = d.data_type_id
              WHERE sg.site_id = ANY($1)
              ORDER BY ae.analysis_entity_id`,
            [siteIds], "entities");
    }

    async _loadAbundances(aeIds) {
        return this._q(
            `SELECT a.abundance_id, a.analysis_entity_id, a.taxon_id, a.abundance,
                    a.abundance_element_id, a.date_updated, e.element_name
               FROM tbl_abundances a
               LEFT JOIN tbl_abundance_elements e
                      ON e.abundance_element_id = a.abundance_element_id
              WHERE a.analysis_entity_id = ANY($1)
              ORDER BY a.analysis_entity_id, a.abundance_id`,
            [aeIds], "abundances");
    }

    async _loadAbundanceModifications(aeIds) {
        return this._q(
            `SELECT am.abundance_id, mt.modification_type_name
               FROM tbl_abundance_modifications am
               JOIN tbl_abundances a ON a.abundance_id = am.abundance_id
               LEFT JOIN tbl_modification_types mt
                      ON mt.modification_type_id = am.modification_type_id
              WHERE a.analysis_entity_id = ANY($1)`,
            [aeIds], "abundance_modifications");
    }

    async _loadMeasuredValues(aeIds) {
        return this._q(
            `SELECT mv.measured_value_id, mv.analysis_entity_id, mv.measured_value, mv.date_updated
               FROM tbl_measured_values mv
              WHERE mv.analysis_entity_id = ANY($1)
              ORDER BY mv.analysis_entity_id, mv.measured_value_id`,
            [aeIds], "measured_values");
    }

    async _loadCeramics(aeIds) {
        return this._q(
            `SELECT c.ceramics_id, c.analysis_entity_id, c.measurement_value,
                    c.ceramics_lookup_id, c.date_updated,
                    cl.name AS lookup_name, cl.description AS lookup_description,
                    cl.method_id AS lookup_method_id
               FROM tbl_ceramics c
               LEFT JOIN tbl_ceramics_lookup cl ON cl.ceramics_lookup_id = c.ceramics_lookup_id
              WHERE c.analysis_entity_id = ANY($1)
              ORDER BY c.analysis_entity_id, c.ceramics_id`,
            [aeIds], "ceramics");
    }

    async _loadIsotopes(aeIds) {
        return this._q(
            `SELECT i.isotope_id, i.analysis_entity_id, i.measurement_value,
                    i.isotope_measurement_id, i.isotope_standard_id,
                    i.isotope_value_specifier_id, i.unit_id, i.date_updated,
                    vs.name AS specifier_name,
                    it.designation AS isotope_type_name,
                    st.isotope_ration, st.international_scale,
                    u.unit_abbrev, u.unit_name
               FROM tbl_isotopes i
               LEFT JOIN tbl_isotope_value_specifiers vs
                      ON vs.isotope_value_specifier_id = i.isotope_value_specifier_id
               LEFT JOIN tbl_isotope_measurements im
                      ON im.isotope_measurement_id = i.isotope_measurement_id
               LEFT JOIN tbl_isotope_types it ON it.isotope_type_id = im.isotope_type_id
               LEFT JOIN tbl_isotope_standards st ON st.isotope_standard_id = i.isotope_standard_id
               LEFT JOIN tbl_units u ON u.unit_id = i.unit_id
              WHERE i.analysis_entity_id = ANY($1)
              ORDER BY i.analysis_entity_id, i.isotope_id`,
            [aeIds], "isotopes");
    }

    /**
     * Analysis values with their class, their type, and every typed subtable.
     *
     * This is the join design F1 asks for: the modules read only
     * `analysis_value`, which drops the qualifier on integer and boolean
     * values and drops the `value_type_item_id` identifying which controlled
     * term a categorical value is. All seven subtables are left-joined here so
     * the caller can take the typed value and keep the text as a fallback.
     */
    async _loadAnalysisValues(aeIds) {
        const rows = await this._q(
            `SELECT av.analysis_value_id,
                    av.analysis_entity_id,
                    av.value_class_id,
                    av.analysis_value,
                    av.boolean_value,
                    av.is_boolean,
                    av.is_uncertain,
                    av.is_undefined,
                    av.is_not_analyzed,
                    av.is_indeterminable,
                    av.is_anomaly,
                    vc.name        AS class_name,
                    vc.description AS class_description,
                    vc.method_id   AS class_method_id,
                    vc.parent_id   AS class_parent_id,
                    vt.value_type_id,
                    vt.name        AS value_type_name,
                    vt.base_type,
                    vt.unit_id     AS value_type_unit_id,
                    vu.unit_abbrev AS value_type_unit_abbrev,
                    m.method_name  AS class_method_name,

                    iv.value       AS integer_value,
                    iv.qualifier   AS integer_qualifier,
                    iv.is_variant  AS integer_is_variant,

                    bv.value       AS boolean_typed_value,
                    bv.qualifier   AS boolean_qualifier,

                    cv.value              AS categorical_value,
                    cv.value_type_item_id AS categorical_item_id,
                    cv.is_variant         AS categorical_is_variant,
                    vti.name              AS categorical_item_name,
                    vti.description       AS categorical_item_description,

                    nv.value      AS numerical_value,
                    nv.qualifier  AS numerical_qualifier,
                    nv.is_variant AS numerical_is_variant,

                    ai.value AS identifier_value,
                    an.value AS note_value,

                    dr.low_value         AS range_low,
                    dr.high_value        AS range_high,
                    dr.low_is_uncertain  AS range_low_uncertain,
                    dr.high_is_uncertain AS range_high_uncertain,
                    dr.low_qualifier     AS range_low_qualifier,
                    dr.high_qualifier    AS range_high_qualifier,
                    dr.age_type_id       AS range_age_type_id,
                    at.age_type          AS range_age_type,
                    se.season_name       AS range_season

               FROM tbl_analysis_values av
               JOIN tbl_value_classes vc ON vc.value_class_id = av.value_class_id
               JOIN tbl_value_types vt ON vt.value_type_id = vc.value_type_id
               LEFT JOIN tbl_units vu ON vu.unit_id = vt.unit_id
               LEFT JOIN tbl_methods m ON m.method_id = vc.method_id

               LEFT JOIN tbl_analysis_integer_values iv ON iv.analysis_value_id = av.analysis_value_id
               LEFT JOIN tbl_analysis_boolean_values bv ON bv.analysis_value_id = av.analysis_value_id
               LEFT JOIN tbl_analysis_categorical_values cv ON cv.analysis_value_id = av.analysis_value_id
               LEFT JOIN tbl_value_type_items vti ON vti.value_type_item_id = cv.value_type_item_id
               LEFT JOIN tbl_analysis_numerical_values nv ON nv.analysis_value_id = av.analysis_value_id
               LEFT JOIN tbl_analysis_identifiers ai ON ai.analysis_value_id = av.analysis_value_id
               LEFT JOIN tbl_analysis_notes an ON an.analysis_value_id = av.analysis_value_id
               LEFT JOIN tbl_analysis_dating_ranges dr ON dr.analysis_value_id = av.analysis_value_id
               LEFT JOIN tbl_age_types at ON at.age_type_id = dr.age_type_id
               LEFT JOIN tbl_seasons se ON se.season_id = dr.season_id

              WHERE av.analysis_entity_id = ANY($1)
              ORDER BY av.analysis_entity_id, av.value_class_id, av.analysis_value_id`,
            [aeIds], "analysis_values");

        for (const r of rows) this._resolveTypedValue(r);
        return rows;
    }

    /**
     * Decides the one value a cell should carry, and says where it came from.
     *
     * Sets `resolved_value`, `resolved_from` (a TYPED_PRECEDENCE member or
     * "text"), `resolved_qualifier` and `has_typed_row`. The text rendering
     * stays on the row as `analysis_value` either way, so nothing is lost by
     * preferring the typed value.
     */
    _resolveTypedValue(r) {
        const present = {
            integer: r.integer_value !== null && r.integer_value !== undefined,
            boolean: r.boolean_typed_value !== null && r.boolean_typed_value !== undefined,
            categorical: r.categorical_item_id !== null && r.categorical_item_id !== undefined,
            numerical: r.numerical_value !== null && r.numerical_value !== undefined,
            identifier: r.identifier_value !== null && r.identifier_value !== undefined,
            dating_range: r.range_low !== null || r.range_high !== null,
            note: r.note_value !== null && r.note_value !== undefined,
        };

        r.has_typed_row = Object.values(present).some(Boolean);
        r.resolved_qualifier = null;

        for (const kind of TYPED_PRECEDENCE) {
            if (!present[kind]) continue;
            r.resolved_from = kind;
            switch (kind) {
                case "integer":
                    r.resolved_value = Number(r.integer_value);
                    r.resolved_qualifier = clean(r.integer_qualifier);
                    return;
                case "boolean":
                    //The text rendering is Swedish ("Ja"/"Nej") while the typed
                    //column is a real boolean (design Q2). Keeping the text is a
                    //deliberate call for now — the typed value stays on the row
                    //as boolean_typed_value for whoever wants it later.
                    r.resolved_value = clean(r.analysis_value) !== null
                        ? clean(r.analysis_value)
                        : r.boolean_typed_value;
                    r.resolved_qualifier = clean(r.boolean_qualifier);
                    return;
                case "categorical":
                    r.resolved_value = clean(r.categorical_item_name)
                        || (r.categorical_value !== null ? Number(r.categorical_value) : null);
                    return;
                case "numerical":
                    r.resolved_value = Number(r.numerical_value);
                    r.resolved_qualifier = clean(r.numerical_qualifier);
                    return;
                case "identifier":
                    r.resolved_value = clean(r.identifier_value);
                    return;
                case "dating_range":
                    r.resolved_value = this._formatRange(r);
                    return;
                case "note":
                    r.resolved_value = clean(r.note_value);
                    return;
            }
        }

        //No typed row: ~30% of analysis values in sead_staging. The text column
        //is the only carrier, so it is reference-grade (design Q2).
        r.resolved_from = "text";
        r.resolved_value = clean(r.analysis_value);
    }

    _formatRange(r) {
        const lo = r.range_low === null || r.range_low === undefined ? null : String(r.range_low);
        const hi = r.range_high === null || r.range_high === undefined ? null : String(r.range_high);
        const q = (v, qual, unc) => {
            if (v === null) return "";
            return `${qual ? qual : ""}${v}${unc ? "?" : ""}`;
        };
        const low = q(lo, r.range_low_qualifier, r.range_low_uncertain);
        const high = q(hi, r.range_high_qualifier, r.range_high_uncertain);
        if (low && high) return low === high ? low : `${low}–${high}`;
        return low || high || null;
    }

    /**
     * Which sheet each analysis entity belongs on, decided by where its data
     * actually is. An entity can appear on more than one sheet — that is real,
     * not an error. Entities that reach no sheet are the completeness finding
     * Phase 0 asks for, so they are collected rather than dropped.
     */
    _route(source) {
        const byEntity = new Map();
        const add = (aeId, sheet) => {
            const k = String(aeId);
            if (!byEntity.has(k)) byEntity.set(k, new Set());
            byEntity.get(k).add(sheet);
        };

        for (const a of source.abundances) add(a.analysis_entity_id, OBSERVATION_SHEETS.ABUNDANCES);
        for (const m of source.measured) add(m.analysis_entity_id, OBSERVATION_SHEETS.MEASUREMENTS);
        for (const c of source.ceramics) add(c.analysis_entity_id, OBSERVATION_SHEETS.CERAMICS);
        for (const i of source.isotopes) add(i.analysis_entity_id, OBSERVATION_SHEETS.ISOTOPES);
        for (const v of source.analysisValues) {
            add(v.analysis_entity_id,
                parseInt(v.class_method_id) === DENDRO_METHOD_ID
                    ? OBSERVATION_SHEETS.DENDROCHRONOLOGY
                    : OBSERVATION_SHEETS.ANALYSIS_VALUES);
        }

        const unclassified = [];
        for (const e of source.entities) {
            if (!byEntity.has(String(e.analysis_entity_id))) {
                unclassified.push({
                    analysis_entity_id: parseInt(e.analysis_entity_id),
                    dataset_id: isBlank(e.dataset_id) ? null : parseInt(e.dataset_id),
                    dataset_name: clean(e.dataset_name),
                    method_id: isBlank(e.method_id) ? null : parseInt(e.method_id),
                    method_name: clean(e.method_name),
                    data_type_name: clean(e.data_type_name),
                });
            }
        }

        return { byEntity, unclassified };
    }
}
