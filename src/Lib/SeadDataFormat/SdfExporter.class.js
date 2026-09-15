import crypto from "crypto";
import {
    SDF_VERSION, ROUNDTRIP, COL_GROUP, SheetBuilder, clean, isBlank, checksum,
} from "./SdfCommon.js";
import SdfSource, { OBSERVATION_SHEETS, DENDRO_METHOD_ID } from "./SdfSource.class.js";

/**
 * Builds a SEAD Data Format bundle for one or more sites.
 *
 * Phase 1 of the design: fetch, flatten and order into a tabular structure plus
 * a manifest. The output is data, not a file. It reuses the consolidated site
 * fetch (getSitePostgres) as the skeleton and queries a few owned tables the
 * JSON API never reads directly (sample/sample-group notes, dataset
 * submissions, sample-group dimensions, dataset masters) so the readable sheets
 * get closer to completeness.
 *
 * What is deliberately not here yet: the _Raw appendix for the remaining
 * uncovered owned tables (D6), the .xlsx rendering (client-side), and the
 * clearinghouse hand-off (Phase 4). Gaps are reported in manifest.coverage
 * rather than hidden.
 */

//The non-empty owned tables the JSON API never reads (design, "Coverage
//against the 68 owned tables"). All of these are now queried directly by
//SdfSource; tbl_analysis_dating_ranges and tbl_analysis_identifiers were not on
//the original list but are populated (7,775 and 41 rows) and are covered too.
const KNOWN_API_GAPS = [
    { table: "tbl_dataset_submissions", handled: true, sheet: "Dataset Submissions" },
    { table: "tbl_sample_notes", handled: true, sheet: "Notes" },
    { table: "tbl_sample_group_notes", handled: true, sheet: "Notes" },
    { table: "tbl_sample_group_dimensions", handled: true, sheet: "Sample Groups" },
    { table: "tbl_dataset_masters", handled: true, sheet: "Datasets" },
    { table: "tbl_analysis_integer_values", handled: true, sheet: "Analysis Values" },
    { table: "tbl_analysis_boolean_values", handled: true, sheet: "Analysis Values" },
    { table: "tbl_analysis_categorical_values", handled: true, sheet: "Analysis Values" },
    { table: "tbl_analysis_numerical_values", handled: true, sheet: "Analysis Values" },
    { table: "tbl_analysis_notes", handled: true, sheet: "Analysis Values" },
    { table: "tbl_analysis_dating_ranges", handled: true, sheet: "Dendrochronology" },
    { table: "tbl_analysis_identifiers", handled: true, sheet: "Analysis Values" },
];

export default class SdfExporter {
    constructor(app) {
        this.app = app;
        this.source = new SdfSource(app);
    }

    /**
     * @param {number[]} siteIds
     * @param {object}   opts
     * @param {"readable"|"complete"} opts.profile
     * @returns {Promise<object>} the SDF bundle
     */
    async export(siteIds, opts = {}) {
        const profile = opts.profile === "complete" ? "complete" : "readable";

        const sites = [];
        for (const siteId of siteIds) {
            const site = await this.app.getSitePostgres(siteId, false);
            if (site) sites.push(site);
        }
        if (sites.length === 0) {
            const err = new Error("No sites found for the given ids");
            err.statusCode = 404;
            throw err;
        }

        const ctx = {
            sites,
            profile,
            vocab: new Map(), //list -> Map(code -> {code,label,description})
            extra: {},        //DB-direct rows keyed by table
        };

        //The structural model: analysis entities, the datasets that own them and
        //every observation hanging off them, read straight from the database.
        //Every Tier B sheet is built from this and none of them reads
        //site.data_groups - see SdfSource for why that structure is unusable as
        //an export source, and deprecated. The sample -> analysis entity edge
        //postProcessSiteData deletes (design F3) is simply present here.
        ctx.source = await this.source.load(sites.map(s => parseInt(s.site_id)));

        await this._loadApiGapTables(ctx);

        const sheets = [];
        sheets.push(this._buildReadme(ctx));
        sheets.push(this._buildSite(ctx));
        sheets.push(this._buildSampleGroups(ctx));
        sheets.push(this._buildSamples(ctx));
        sheets.push(this._buildDatasets(ctx));
        sheets.push(this._buildSiteLocations(ctx));
        sheets.push(this._buildSiteOtherRecords(ctx));
        sheets.push(this._buildSampleCoordinates(ctx));
        sheets.push(this._buildSampleFeatures(ctx));
        sheets.push(this._buildReferences(ctx));
        sheets.push(this._buildNotes(ctx));
        sheets.push(this._buildDatasetContacts(ctx));
        sheets.push(this._buildDatasetSubmissions(ctx));
        sheets.push(this._buildAbundances(ctx));
        sheets.push(this._buildMeasurements(ctx));
        sheets.push(this._buildDendrochronology(ctx));
        sheets.push(this._buildCeramics(ctx));
        sheets.push(this._buildAnalysisValues(ctx));
        sheets.push(this._buildDating(ctx));
        sheets.push(this._buildPrepMethods(ctx));
        sheets.push(this._buildIdentificationLevels(ctx));
        sheets.push(this._buildIsotopes(ctx));
        sheets.push(this._buildTaxa(ctx));
        sheets.push(this._buildBibliography(ctx));
        sheets.push(this._buildMethods(ctx));
        sheets.push(this._buildVocabularies(ctx));

        const finalized = sheets.map(s => s.finalize());
        const manifest = this._buildManifest(ctx, finalized);

        return {
            sdf_version: SDF_VERSION,
            profile,
            exporter: `${this.app.appName}-${this.app.appVersion}`,
            manifest,
            sheets: finalized,
        };
    }

    // ---------------------------------------------------------------- helpers

    _voc(ctx, list, code, label, description = null) {
        if (isBlank(code) || isBlank(label)) return clean(label) || clean(code);
        if (!ctx.vocab.has(list)) ctx.vocab.set(list, new Map());
        const m = ctx.vocab.get(list);
        const key = String(code);
        if (!m.has(key)) m.set(key, { code: key, label: String(label), description: clean(description) });
        return String(label);
    }

    /** Numeric or null - pg returns numeric/decimal as strings, and NaN must never reach a cell. */
    _num(v) {
        if (isBlank(v)) return null;
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
    }

    _siteName(site) {
        return clean(site.site_name) || `site ${site.site_id}`;
    }

    /**
     * analysis_entity_id -> { siteName, sampleName, physicalSampleId, datasetId }
     * for the sheets that are not built through _entityRow (Dating, Prep
     * Methods, Identification Levels).
     *
     * This reads the structural model loaded by SdfSource, so it no longer has
     * to work around postProcessSiteData having deleted the sample -> analysis
     * entity edge (design F3) - the edge is simply present.
     */
    _analysisEntityContext(ctx) {
        if (ctx._aeContext) return ctx._aeContext;
        const map = new Map();
        for (const [key, e] of ctx.source.entityById) {
            map.set(key, {
                siteName: this._siteNameById(ctx, e.site_id),
                sampleName: clean(e.sample_name),
                physicalSampleId: isBlank(e.physical_sample_id) ? null : parseInt(e.physical_sample_id),
                datasetId: isBlank(e.dataset_id) ? null : parseInt(e.dataset_id),
                datasetName: clean(e.dataset_name),
                methodName: clean(e.method_name),
            });
        }
        ctx._aeContext = map;
        return map;
    }

    async _loadApiGapTables(ctx) {
        const siteIds = ctx.sites.map(s => parseInt(s.site_id)).filter(Boolean);
        const sgIds = [];
        const sampleIds = [];
        const datasetIds = [];
        for (const site of ctx.sites) {
            for (const sg of site.sample_groups || []) {
                sgIds.push(parseInt(sg.sample_group_id));
                for (const ps of sg.physical_samples || []) sampleIds.push(parseInt(ps.physical_sample_id));
            }
            for (const ds of site.datasets || []) datasetIds.push(parseInt(ds.dataset_id));
        }

        const q = async (sql, params) => {
            try {
                const r = await this.app.query(sql, params);
                return r.rows;
            } catch (e) {
                console.warn("SDF: optional gap-table query failed:", e.message);
                return [];
            }
        };

        ctx.extra.sample_notes = sampleIds.length ? await q(
            `SELECT sample_note_id, physical_sample_id, note_type, note, date_updated
               FROM tbl_sample_notes WHERE physical_sample_id = ANY($1)`, [sampleIds]) : [];

        ctx.extra.sample_group_notes = sgIds.length ? await q(
            `SELECT sample_group_note_id, sample_group_id, note, date_updated
               FROM tbl_sample_group_notes WHERE sample_group_id = ANY($1)`, [sgIds]) : [];

        ctx.extra.sample_group_dimensions = sgIds.length ? await q(
            `SELECT d.sample_group_dimension_id, d.sample_group_id, d.dimension_id,
                    d.dimension_value, dim.dimension_name, d.date_updated
               FROM tbl_sample_group_dimensions d
               JOIN tbl_dimensions dim ON dim.dimension_id = d.dimension_id
              WHERE d.sample_group_id = ANY($1)`, [sgIds]) : [];

        ctx.extra.dataset_submissions = datasetIds.length ? await q(
            `SELECT s.dataset_submission_id, s.dataset_id, s.submission_type_id,
                    t.submission_type, s.contact_id, s.date_submitted, s.notes, s.date_updated
               FROM tbl_dataset_submissions s
               LEFT JOIN tbl_dataset_submission_types t ON t.submission_type_id = s.submission_type_id
              WHERE s.dataset_id = ANY($1)`, [datasetIds]) : [];

        try {
            const dt = await this.app.query(`SELECT data_type_id, data_type_name, definition FROM tbl_data_types`, []);
            ctx.extra.data_types = new Map(dt.rows.map(r => [String(r.data_type_id), r]));
        } catch (e) {
            ctx.extra.data_types = new Map();
        }

        //Analysis-entity ids for this site set. The dating tables, prep methods and identification
        //levels all hang off analysis entities, and the JSON API either aggregates them past the
        //point of recovery (dating) or does not carry them at all (the other two), so SDF queries
        //them directly.
        //From the structural model, not the site document: the site document's
        //dataset list is whatever the API's allowlisted modules assembled
        //(design F2), so deriving the scope from it would carry that gap into
        //the dating, prep-method and identification-level sheets too.
        const aeIds = ctx.source.aeIds;
        ctx.extra.ae_ids = aeIds;

        //--- dating, one query per source table so every column keeps a real binding -------------
        ctx.extra.geochronology = aeIds.length ? await q(
            `SELECT g.geochron_id, g.analysis_entity_id, g.lab_number, g.age, g.error_older,
                    g.error_younger, g.delta_13c, g.notes, g.dating_uncertainty_id,
                    l.lab_name, l.international_lab_id, u.uncertainty, g.date_updated
               FROM tbl_geochronology g
               LEFT JOIN tbl_dating_labs l ON l.dating_lab_id = g.dating_lab_id
               LEFT JOIN tbl_dating_uncertainty u ON u.dating_uncertainty_id = g.dating_uncertainty_id
              WHERE g.analysis_entity_id = ANY($1)`, [aeIds]) : [];

        ctx.extra.relative_dates = aeIds.length ? await q(
            `SELECT rd.relative_date_id, rd.analysis_entity_id, rd.relative_age_id, rd.notes,
                    rd.dating_uncertainty_id, ra.relative_age_name, ra.abbreviation,
                    ra.c14_age_older, ra.c14_age_younger, ra.cal_age_older, ra.cal_age_younger,
                    u.uncertainty, rd.date_updated
               FROM tbl_relative_dates rd
               LEFT JOIN tbl_relative_ages ra ON ra.relative_age_id = rd.relative_age_id
               LEFT JOIN tbl_dating_uncertainty u ON u.dating_uncertainty_id = rd.dating_uncertainty_id
              WHERE rd.analysis_entity_id = ANY($1)`, [aeIds]) : [];

        //tbl_dendro_dates and tbl_analysis_dating_ranges are not fetched: both
        //are fully subsumed by the method 10 value classes that build the
        //Dendrochronology sheet (D11). See the notes in _buildDating.

        ctx.extra.entity_ages = aeIds.length ? await q(
            `SELECT a.analysis_entity_age_id, a.analysis_entity_id, a.age, a.age_older,
                    a.age_younger, a.age_range, a.dating_specifier, a.chronology_id,
                    c.chronology_name, c.age_model, a.date_updated
               FROM tbl_analysis_entity_ages a
               LEFT JOIN tbl_chronologies c ON c.chronology_id = a.chronology_id
              WHERE a.analysis_entity_id = ANY($1)`, [aeIds]) : [];

        //--- previously uncovered owned tables ---------------------------------------------------
        ctx.extra.prep_methods = aeIds.length ? await q(
            `SELECT p.analysis_entity_prep_method_id, p.analysis_entity_id, p.method_id,
                    m.method_name, m.description AS method_description, p.date_updated
               FROM tbl_analysis_entity_prep_methods p
               LEFT JOIN tbl_methods m ON m.method_id = p.method_id
              WHERE p.analysis_entity_id = ANY($1)`, [aeIds]) : [];

        ctx.extra.ident_levels = aeIds.length ? await q(
            `SELECT il.abundance_ident_level_id, il.abundance_id, a.analysis_entity_id,
                    il.identification_level_id, l.identification_level_abbrev,
                    l.identification_level_name, l.notes, il.date_updated
               FROM tbl_abundance_ident_levels il
               JOIN tbl_abundances a ON a.abundance_id = il.abundance_id
               LEFT JOIN tbl_identification_levels l
                      ON l.identification_level_id = il.identification_level_id
              WHERE a.analysis_entity_id = ANY($1)`, [aeIds]) : [];

        const masterIds = [...new Set((ctx.sites.flatMap(s => s.datasets || []))
            .map(d => d.master_set_id).filter(v => !isBlank(v)))];
        ctx.extra.dataset_masters = masterIds.length ? await q(
            `SELECT master_set_id, master_name, master_notes, url, master_set_uuid, date_updated
               FROM tbl_dataset_masters WHERE master_set_id = ANY($1)`, [masterIds]) : [];
        ctx.extra.dataset_masters_by_id = new Map(
            ctx.extra.dataset_masters.map(m => [String(m.master_set_id), m]));
    }

    // ---------------------------------------------------------------- Tier A

    _buildReadme(ctx) {
        const b = new SheetBuilder("README", "A", "n/a", {
            note: "What this file is and how it round-trips. Not read on import.",
        });
        b.col("Item", { roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.OWN });
        b.col("Value", { roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.OWN });
        const add = (k, v) => b.addRow({ Item: k, Value: v });
        add("Format", `SEAD Data Format ${SDF_VERSION}`);
        add("Unit of transfer", "One site bundle per site (D1). Every sheet carries a site column.");
        add("Editing", "Edit cells in sheets marked 'editable'. Columns prefixed with _ are hidden ID keys — do not touch except to write a NEW-n token for a new row.");
        add("Deleting", "Set the Action column to 'delete'. An absent row is never treated as a deletion (D12).");
        add("New rows", "Leave the _id blank or type NEW-1, NEW-2, ... Other sheets may point at that token.");
        add("Binding", "Columns bind by header text, recorded in _Manifest. Renaming a header breaks the binding and is reported as an error, not applied silently (D4).");
        add("Round-trip", "editable = written back · reference = real data, not editable here · derived = computed, ignored on import (D5).");
        add("Vocabularies", "Values in dropdown columns must exist in the Vocabularies sheet. An unknown value becomes a curator proposal, never an automatic insert (D7).");
        add("Import", "Produces a validation report and a diff. It writes nothing to the database (D8).");
        return b;
    }

    _buildSite(ctx) {
        const b = new SheetBuilder("Site", "A", "one row per site");
        b.idCol("_site_id", { title: "_site_id", source: "public.tbl_sites.site_id", table: "tbl_sites", type: "integer" });
        b.idCol("_site_uuid", { title: "_site_uuid", source: "public.tbl_sites.site_uuid" });
        b.actionCol();
        b.col("Site name", { source: "public.tbl_sites.site_name", table: "tbl_sites", type: "text" });
        b.col("National site identifier", { source: "public.tbl_sites.national_site_identifier", type: "text" });
        b.col("Latitude (WGS84)", { source: "public.tbl_sites.latitude_dd", type: "number" });
        b.col("Longitude (WGS84)", { source: "public.tbl_sites.longitude_dd", type: "number" });
        b.col("Altitude (m)", { source: "public.tbl_sites.altitude", type: "number" });
        b.col("Location accuracy", { source: "public.tbl_sites.site_location_accuracy", type: "text" });
        b.col("Site description", { source: "public.tbl_sites.site_description", type: "text" });
        b.col("Preservation status", { source: "public.tbl_sites.site_preservation_status_id", type: "text" });
        b.col("Map link", { source: "derived:map_link", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.DERIVED, locked: true });
        b.col("Last updated", { source: "public.tbl_sites.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });

        for (const site of ctx.sites) {
            const lat = clean(site.latitude_dd), lon = clean(site.longitude_dd);
            b.addRow({
                _site_id: parseInt(site.site_id),
                _site_uuid: clean(site.site_uuid),
                Action: "",
                "Site name": clean(site.site_name),
                "National site identifier": clean(site.national_site_identifier),
                "Latitude (WGS84)": lat === null ? null : Number(lat),
                "Longitude (WGS84)": lon === null ? null : Number(lon),
                "Altitude (m)": clean(site.altitude) === null ? null : Number(site.altitude),
                "Location accuracy": clean(site.site_location_accuracy),
                "Site description": clean(site.site_description),
                "Preservation status": clean(site.site_preservation_status_id),
                "Map link": (lat !== null && lon !== null)
                    ? `https://www.google.com/maps/search/?api=1&query=${lat},${lon}` : null,
                "Last updated": clean(site.date_updated),
            });
        }
        return b;
    }

    _buildSampleGroups(ctx) {
        const b = new SheetBuilder("Sample Groups", "A", "one row per sample group", {
            note: "Coordinates and dimensions are pivoted where single-valued; the exporter reports any group where they are not.",
        });
        b.idCol("_sample_group_id", { source: "public.tbl_sample_groups.sample_group_id", table: "tbl_sample_groups", type: "integer" });
        b.idCol("_site_id", { source: "public.tbl_sample_groups.site_id", type: "integer" });
        b.idCol("_sample_group_uuid", { source: "public.tbl_sample_groups.sample_group_uuid" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample group name", { source: "public.tbl_sample_groups.sample_group_name", type: "text" });
        b.col("Sampling context", { source: "public.tbl_sample_groups.sampling_context_id", type: "enum", vocab: "sampling_context" });
        b.col("Sampling method", { source: "public.tbl_sample_groups.method_id", type: "enum", vocab: "method" });

        for (const site of ctx.sites) {
            const sName = this._siteName(site);
            for (const sg of site.sample_groups || []) {
                const row = {
                    _sample_group_id: parseInt(sg.sample_group_id),
                    _site_id: parseInt(site.site_id),
                    _sample_group_uuid: clean(sg.sample_group_uuid),
                    Action: "",
                    Site: sName,
                    "Sample group name": clean(sg.sample_group_name),
                    "Sampling context": this._pivotType(ctx, "sampling_context", sg.sampling_context,
                        sg.sampling_context_id, r => r.sampling_context, r => r.description),
                    "Sampling method": this._voc(ctx, "method", sg.method_id, this._methodLabel(site, sg.method_id) || sg.method_id),
                };

                this._absorbByType(ctx, b, row, sg.descriptions || [], {
                    prefix: "Description", table: "tbl_sample_group_descriptions",
                    typeName: d => d.type_name, value: d => d.group_description,
                });
                this._absorbByType(ctx, b, row, ctx.extra.sample_group_dimensions
                    .filter(d => String(d.sample_group_id) === String(sg.sample_group_id)), {
                    prefix: "Dimension", table: "tbl_sample_group_dimensions",
                    typeName: d => d.dimension_name, value: d => d.dimension_value,
                });
                this._absorbCoordinates(ctx, b, row, sg.coordinates || [], "tbl_sample_group_coordinates");

                const notes = ctx.extra.sample_group_notes
                    .filter(n => String(n.sample_group_id) === String(sg.sample_group_id));
                if (notes.length) {
                    b.col("Note", { source: "public.tbl_sample_group_notes.note", type: "text", group: COL_GROUP.PIVOT });
                    row["Note"] = notes.map(n => clean(n.note)).filter(Boolean).join(" | ");
                }

                b.col("Last updated", { source: "public.tbl_sample_groups.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });
                row["Last updated"] = clean(sg.date_updated);
                b.addRow(row);
            }
        }
        return b;
    }

    _buildSamples(ctx) {
        const b = new SheetBuilder("Samples", "A", "one row per physical sample", {
            note: "Coordinates and features live on their own sheets (multi-valued). Sample names are text — some carry leading zeros.",
        });
        b.idCol("_physical_sample_id", { source: "public.tbl_physical_samples.physical_sample_id", table: "tbl_physical_samples", type: "integer" });
        b.idCol("_sample_group_id", { source: "public.tbl_physical_samples.sample_group_id", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample group", { source: "context:tbl_sample_groups.sample_group_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample name", { source: "public.tbl_physical_samples.sample_name", type: "text" });
        b.col("Sample type", { source: "public.tbl_physical_samples.sample_type_id", type: "enum", vocab: "sample_type" });
        b.col("Date sampled", { source: "public.tbl_physical_samples.date_sampled", type: "text" });

        for (const site of ctx.sites) {
            const sName = this._siteName(site);
            for (const sg of site.sample_groups || []) {
                const sgName = clean(sg.sample_group_name);
                for (const ps of sg.physical_samples || []) {
                    const row = {
                        _physical_sample_id: parseInt(ps.physical_sample_id),
                        _sample_group_id: parseInt(sg.sample_group_id),
                        Action: "",
                        Site: sName,
                        "Sample group": sgName,
                        "Sample name": clean(ps.sample_name) === null ? null : String(ps.sample_name),
                        "Sample type": this._voc(ctx, "sample_type", ps.sample_type_id, ps.sample_type_name, ps.sample_type_description) || clean(ps.sample_type_id),
                        "Date sampled": clean(ps.date_sampled),
                    };

                    this._absorbByType(ctx, b, row, ps.alt_refs || [], {
                        prefix: "Alt ref", table: "tbl_sample_alt_refs",
                        typeName: d => d.alt_ref_type, value: d => d.alt_ref,
                    });
                    this._absorbByType(ctx, b, row, ps.descriptions || [], {
                        prefix: "Description", table: "tbl_sample_descriptions",
                        typeName: d => d.type_name, value: d => d.description,
                    });
                    this._absorbByType(ctx, b, row, ps.dimensions || [], {
                        prefix: "Dimension", table: "tbl_sample_dimensions",
                        typeName: d => d.dimension_name || d.dimension_abbrev, value: d => d.dimension_value ?? d.measurement,
                    });
                    this._absorbByType(ctx, b, row, ps.locations || [], {
                        prefix: "Location", table: "tbl_sample_locations",
                        typeName: d => d.location_type, value: d => d.location,
                    });
                    if ((ps.horizons || []).length) {
                        b.col("Horizon", { source: "pivot:tbl_sample_horizons:horizon", type: "text", group: COL_GROUP.PIVOT });
                        row["Horizon"] = ps.horizons.map(h => (typeof h === "object" ? h.horizon_name : h)).join(" | ");
                    }

                    b.col("Last updated", { source: "public.tbl_physical_samples.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });
                    row["Last updated"] = clean(ps.date_updated);
                    b.addRow(row);
                }
            }
        }
        return b;
    }

    _buildDatasets(ctx) {
        const b = new SheetBuilder("Datasets", "A", "one row per dataset", {
            note: "Contacts and submissions are multi-valued and live on their own sheets. Legacy dendro datasets are listed verbatim in this profile.",
        });
        b.idCol("_dataset_id", { source: "public.tbl_datasets.dataset_id", table: "tbl_datasets", type: "integer" });
        b.idCol("_dataset_uuid", { source: "public.tbl_datasets.dataset_uuid" });
        b.idCol("_master_set_id", { source: "public.tbl_datasets.master_set_id", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Dataset name", { source: "public.tbl_datasets.dataset_name", type: "text" });
        b.col("Method", { source: "public.tbl_datasets.method_id", type: "enum", vocab: "method" });
        b.col("Data type", { source: "public.tbl_datasets.data_type_id", type: "enum", vocab: "data_type" });
        b.col("Project", { source: "public.tbl_datasets.project_id", type: "text" });
        b.col("Reference", { source: "public.tbl_datasets.biblio_id", type: "text" });
        b.col("Master set", { source: "public.tbl_dataset_masters.master_name", type: "text", table: "tbl_dataset_masters" });
        b.col("Last updated", { source: "public.tbl_datasets.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });

        for (const site of ctx.sites) {
            const sName = this._siteName(site);
            for (const ds of site.datasets || []) {
                const master = ctx.extra.dataset_masters_by_id.get(String(ds.master_set_id));
                b.addRow({
                    _dataset_id: parseInt(ds.dataset_id),
                    _dataset_uuid: clean(ds.dataset_uuid),
                    _master_set_id: isBlank(ds.master_set_id) ? null : parseInt(ds.master_set_id),
                    Action: "",
                    Site: sName,
                    "Dataset name": clean(ds.dataset_name),
                    Method: this._voc(ctx, "method", ds.method_id, this._methodLabel(site, ds.method_id) || ds.method_id),
                    "Data type": this._voc(ctx, "data_type", ds.data_type_id,
                        (ctx.extra.data_types.get(String(ds.data_type_id)) || {}).data_type_name || ds.data_type_id,
                        (ctx.extra.data_types.get(String(ds.data_type_id)) || {}).definition),
                    Project: clean(ds.project_id),
                    Reference: this._biblioLabel(site, ds.biblio_id),
                    "Master set": master ? clean(master.master_name) : null,
                    "Last updated": clean(ds.date_updated),
                });
            }
        }
        return b;
    }

    _buildSiteLocations(ctx) {
        const b = new SheetBuilder("Site Locations", "A", "one row per site x location", {
            note: "tbl_site_locations joined to tbl_locations. Up to five per type, so it cannot fold into the Site sheet.",
        });
        b.idCol("_location_id", { source: "public.tbl_locations.location_id", table: "tbl_site_locations", type: "integer" });
        b.idCol("_site_id", { source: "public.tbl_site_locations.site_id", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Location type", { source: "public.tbl_location_types.location_type", type: "enum", vocab: "location_type" });
        b.col("Location name", { source: "public.tbl_locations.location_name", type: "text" });
        b.col("Location description", { source: "public.tbl_locations.location_description", roundtrip: ROUNDTRIP.REFERENCE, type: "text", locked: true });

        for (const site of ctx.sites) {
            const sName = this._siteName(site);
            for (const loc of site.location || []) {
                this._voc(ctx, "location_type", loc.location_type_id, loc.location_type, loc.location_description);
                b.addRow({
                    _location_id: parseInt(loc.location_id),
                    _site_id: parseInt(site.site_id),
                    Action: "",
                    Site: sName,
                    "Location type": clean(loc.location_type),
                    "Location name": clean(loc.location_name),
                    "Location description": clean(loc.location_description),
                });
            }
        }
        return b;
    }

    _buildSiteOtherRecords(ctx) {
        const b = new SheetBuilder("Site Other Records", "A", "one row per record");
        b.idCol("_site_other_records_id", { source: "public.tbl_site_other_records.site_other_records_id", table: "tbl_site_other_records", type: "integer" });
        b.idCol("_site_id", { source: "public.tbl_site_other_records.site_id", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Record type", { source: "public.tbl_record_types.record_type_name", type: "enum", vocab: "record_type" });
        b.col("Description", { source: "public.tbl_site_other_records.description", type: "text" });
        b.col("Reference", { source: "public.tbl_site_other_records.biblio_id", type: "text" });

        for (const site of ctx.sites) {
            const sName = this._siteName(site);
            for (const rec of site.other_records || []) {
                this._voc(ctx, "record_type", rec.record_type_id, rec.record_type_name || rec.record_type, rec.record_type_description);
                b.addRow({
                    _site_other_records_id: parseInt(rec.site_other_records_id),
                    _site_id: parseInt(site.site_id),
                    Action: "",
                    Site: sName,
                    "Record type": clean(rec.record_type_name || rec.record_type),
                    Description: clean(rec.description),
                    Reference: this._biblioLabel(site, rec.biblio_id),
                });
            }
        }
        return b;
    }

    _buildSampleCoordinates(ctx) {
        const b = new SheetBuilder("Sample Coordinates", "A", "one row per sample x dimension", {
            note: "tbl_sample_coordinates joined to coordinate method dimensions. Two rows per dimension exist in ~891 cases, so this cannot fold into Samples.",
        });
        b.idCol("_physical_sample_id", { source: "public.tbl_sample_coordinates.physical_sample_id", table: "tbl_sample_coordinates", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample", { source: "context:tbl_physical_samples.sample_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Dimension", { source: "public.tbl_dimensions.dimension_name", type: "enum", vocab: "dimension" });
        b.col("Coordinate method", { source: "public.tbl_sample_coordinates.coordinate_method_id", type: "text" });
        b.col("Measurement", { source: "public.tbl_sample_coordinates.measurement", type: "number" });
        b.col("Accuracy", { source: "public.tbl_sample_coordinates.accuracy", type: "text" });

        for (const site of ctx.sites) {
            const sName = this._siteName(site);
            for (const sg of site.sample_groups || []) {
                for (const ps of sg.physical_samples || []) {
                    for (const c of ps.coordinates || []) {
                        const dim = this._dimensionLabel(site, c.dimension_id);
                        this._voc(ctx, "dimension", c.dimension_id, dim || c.dimension_id);
                        b.addRow({
                            _physical_sample_id: parseInt(ps.physical_sample_id),
                            Action: "",
                            Site: sName,
                            Sample: clean(ps.sample_name) === null ? null : String(ps.sample_name),
                            Dimension: dim || clean(c.dimension_id),
                            "Coordinate method": clean(c.coordinate_method_id),
                            Measurement: isBlank(c.measurement) ? null : Number(c.measurement),
                            Accuracy: clean(c.accuracy),
                        });
                    }
                }
            }
        }
        return b;
    }

    _buildSampleFeatures(ctx) {
        const b = new SheetBuilder("Sample Features", "A", "one row per sample x feature");
        b.idCol("_physical_sample_feature_id", { source: "public.tbl_physical_sample_features.physical_sample_feature_id", table: "tbl_physical_sample_features", type: "integer" });
        b.idCol("_physical_sample_id", { source: "public.tbl_physical_sample_features.physical_sample_id", type: "integer" });
        b.idCol("_feature_id", { source: "public.tbl_physical_sample_features.feature_id", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample", { source: "context:tbl_physical_samples.sample_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Feature type", { source: "public.tbl_feature_types.feature_type_name", type: "enum", vocab: "feature_type" });
        b.col("Feature name", { source: "public.tbl_features.feature_name", type: "text" });
        b.col("Feature description", { source: "public.tbl_features.feature_description", type: "text" });

        for (const site of ctx.sites) {
            const sName = this._siteName(site);
            for (const sg of site.sample_groups || []) {
                for (const ps of sg.physical_samples || []) {
                    for (const f of ps.features || []) {
                        this._voc(ctx, "feature_type", f.feature_type_id, f.feature_type_name, f.feature_type_description);
                        b.addRow({
                            _physical_sample_feature_id: parseInt(f.physical_sample_feature_id),
                            _physical_sample_id: parseInt(ps.physical_sample_id),
                            _feature_id: isBlank(f.feature_id) ? null : parseInt(f.feature_id),
                            Action: "",
                            Site: sName,
                            Sample: clean(ps.sample_name) === null ? null : String(ps.sample_name),
                            "Feature type": clean(f.feature_type_name),
                            "Feature name": clean(f.feature_name),
                            "Feature description": clean(f.feature_description),
                        });
                    }
                }
            }
        }
        return b;
    }

    _buildReferences(ctx) {
        const b = new SheetBuilder("References", "A", "one row per parent x citation", {
            note: "tbl_site_references and tbl_sample_group_references unified under a Scope discriminator.",
        });
        b.idCol("_reference_id", { source: "public.tbl_site_references.site_reference_id / tbl_sample_group_references.sample_group_reference_id", type: "integer" });
        b.idCol("_parent_id", { source: "site_id or sample_group_id, per Scope", type: "integer" });
        b.idCol("_biblio_id", { source: "public.tbl_biblio.biblio_id", type: "integer" });
        b.actionCol();
        b.col("Scope", { source: "discriminator", type: "enum", vocab: "reference_scope" });
        b.col("Parent", { source: "context: site or sample group name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Citation", { source: "public.tbl_biblio.full_reference", roundtrip: ROUNDTRIP.REFERENCE, type: "text", locked: true });

        for (const site of ctx.sites) {
            const sName = this._siteName(site);
            for (const ref of site.biblio || []) {
                b.addRow({
                    _reference_id: isBlank(ref.site_reference_id) ? null : parseInt(ref.site_reference_id),
                    _parent_id: parseInt(site.site_id),
                    _biblio_id: isBlank(ref.biblio_id) ? null : parseInt(ref.biblio_id),
                    Action: "",
                    Scope: "site",
                    Parent: sName,
                    Citation: this._biblioText(ref),
                });
            }
            for (const sg of site.sample_groups || []) {
                for (const ref of sg.biblio || []) {
                    b.addRow({
                        _reference_id: isBlank(ref.sample_group_reference_id) ? null : parseInt(ref.sample_group_reference_id),
                        _parent_id: parseInt(sg.sample_group_id),
                        _biblio_id: isBlank(ref.biblio_id) ? null : parseInt(ref.biblio_id),
                        Action: "",
                        Scope: "sample_group",
                        Parent: clean(sg.sample_group_name),
                        Citation: this._biblioText(ref),
                    });
                }
            }
        }
        this._voc(ctx, "reference_scope", "site", "site");
        this._voc(ctx, "reference_scope", "sample_group", "sample_group");
        return b;
    }

    _buildNotes(ctx) {
        const b = new SheetBuilder("Notes", "A", "one row per parent x note", {
            note: "tbl_sample_notes and tbl_sample_group_notes unified under a Scope discriminator. Field notes were missing from every export before SDF.",
        });
        b.idCol("_note_id", { source: "public.tbl_sample_notes.sample_note_id / tbl_sample_group_notes.sample_group_note_id", type: "integer" });
        b.idCol("_parent_id", { source: "physical_sample_id or sample_group_id, per Scope", type: "integer" });
        b.actionCol();
        b.col("Scope", { source: "discriminator", type: "enum", vocab: "note_scope" });
        b.col("Parent", { source: "context: sample or sample group name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Note type", { source: "public.tbl_sample_notes.note_type", type: "text" });
        b.col("Note", { source: "public.tbl_sample_notes.note", type: "text" });

        const sampleName = new Map();
        const sgName = new Map();
        for (const site of ctx.sites) {
            for (const sg of site.sample_groups || []) {
                sgName.set(String(sg.sample_group_id), clean(sg.sample_group_name));
                for (const ps of sg.physical_samples || []) {
                    sampleName.set(String(ps.physical_sample_id), clean(ps.sample_name) === null ? null : String(ps.sample_name));
                }
            }
        }
        for (const n of ctx.extra.sample_notes) {
            b.addRow({
                _note_id: parseInt(n.sample_note_id),
                _parent_id: parseInt(n.physical_sample_id),
                Action: "",
                Scope: "sample",
                Parent: sampleName.get(String(n.physical_sample_id)) || null,
                "Note type": clean(n.note_type),
                Note: clean(n.note),
            });
        }
        for (const n of ctx.extra.sample_group_notes) {
            b.addRow({
                _note_id: parseInt(n.sample_group_note_id),
                _parent_id: parseInt(n.sample_group_id),
                Action: "",
                Scope: "sample_group",
                Parent: sgName.get(String(n.sample_group_id)) || null,
                "Note type": null,
                Note: clean(n.note),
            });
        }
        this._voc(ctx, "note_scope", "sample", "sample");
        this._voc(ctx, "note_scope", "sample_group", "sample_group");
        return b;
    }

    _buildDatasetContacts(ctx) {
        const b = new SheetBuilder("Dataset Contacts", "A", "one row per dataset x person");
        b.idCol("_dataset_contact_id", { source: "public.tbl_dataset_contacts.dataset_contact_id", type: "integer" });
        b.idCol("_dataset_id", { source: "public.tbl_dataset_contacts.dataset_id", type: "integer" });
        b.idCol("_contact_id", { source: "public.tbl_dataset_contacts.contact_id", type: "integer" });
        b.actionCol();
        b.col("Dataset", { source: "context:tbl_datasets.dataset_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Role", { source: "public.tbl_contact_types.contact_type_name", type: "enum", vocab: "contact_type" });
        b.col("Contact", { source: "public.tbl_contacts (name)", type: "text" });

        for (const site of ctx.sites) {
            for (const ds of site.datasets || []) {
                for (const c of ds.contacts || []) {
                    b.addRow({
                        _dataset_contact_id: isBlank(c.dataset_contact_id) ? null : parseInt(c.dataset_contact_id),
                        _dataset_id: parseInt(ds.dataset_id),
                        _contact_id: isBlank(c.contact_id) ? null : parseInt(c.contact_id),
                        Action: "",
                        Dataset: clean(ds.dataset_name),
                        Role: this._voc(ctx, "contact_type", c.contact_type_id ?? c.contact_type_name, c.contact_type_name || c.role || c.contact_type_id),
                        Contact: clean(c.name || c.contact_name || [c.first_name, c.last_name].filter(Boolean).join(" ") || null),
                    });
                }
            }
        }
        return b;
    }

    _buildDatasetSubmissions(ctx) {
        const b = new SheetBuilder("Dataset Submissions", "A", "one row per submission", {
            note: "tbl_dataset_submissions — submission provenance, absent from every export before SDF.",
        });
        b.idCol("_dataset_submission_id", { source: "public.tbl_dataset_submissions.dataset_submission_id", table: "tbl_dataset_submissions", type: "integer" });
        b.idCol("_dataset_id", { source: "public.tbl_dataset_submissions.dataset_id", type: "integer" });
        b.idCol("_contact_id", { source: "public.tbl_dataset_submissions.contact_id", type: "integer" });
        b.actionCol();
        b.col("Dataset", { source: "context:tbl_datasets.dataset_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Submission type", { source: "public.tbl_dataset_submission_types.submission_type", type: "enum", vocab: "submission_type" });
        b.col("Date submitted", { source: "public.tbl_dataset_submissions.date_submitted", type: "text" });
        b.col("Notes", { source: "public.tbl_dataset_submissions.notes", type: "text" });

        const dsName = new Map();
        for (const site of ctx.sites) for (const ds of site.datasets || []) dsName.set(String(ds.dataset_id), clean(ds.dataset_name));
        for (const s of ctx.extra.dataset_submissions) {
            this._voc(ctx, "submission_type", s.submission_type_id, s.submission_type || s.submission_type_id);
            b.addRow({
                _dataset_submission_id: parseInt(s.dataset_submission_id),
                _dataset_id: parseInt(s.dataset_id),
                _contact_id: isBlank(s.contact_id) ? null : parseInt(s.contact_id),
                Action: "",
                Dataset: dsName.get(String(s.dataset_id)) || null,
                "Submission type": clean(s.submission_type),
                "Date submitted": clean(s.date_submitted),
                Notes: clean(s.notes),
            });
        }
        return b;
    }

    // ---------------------------------------------------------------- Tier B

    /**
     * The shared identity + context block for every Tier B sheet.
     *
     * Identity is the real thing: the analysis entity, the dataset that owns it
     * and the physical sample it came from, all straight out of
     * tbl_analysis_entities. The previous version reached these through
     * site.data_groups, where two of three value-class modules carried no
     * analysis_entity_id at all and every row exported with a null primary key.
     */
    _observationSheet(name, grain, note) {
        const b = new SheetBuilder(name, "B", grain, { note });
        b.idCol("_analysis_entity_id", { source: "public.tbl_analysis_entities.analysis_entity_id", table: "tbl_analysis_entities", type: "integer" });
        b.idCol("_dataset_id", { source: "public.tbl_analysis_entities.dataset_id", table: "tbl_analysis_entities", type: "integer" });
        b.idCol("_physical_sample_id", { source: "public.tbl_analysis_entities.physical_sample_id", table: "tbl_analysis_entities", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample", { source: "context:tbl_physical_samples.sample_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Dataset", { source: "context:tbl_datasets.dataset_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Method", { source: "context:tbl_methods.method_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        return b;
    }

    _siteNameById(ctx, siteId) {
        if (!ctx._siteNames) {
            ctx._siteNames = new Map(ctx.sites.map(s => [String(s.site_id), this._siteName(s)]));
        }
        return ctx._siteNames.get(String(siteId)) || null;
    }

    /**
     * Identity and context for one analysis entity, from the structural model.
     * Every Tier B row starts here, so no sheet has to invent an identity.
     */
    _entityRow(ctx, aeId) {
        const e = ctx.source.entityById.get(String(aeId));
        if (!e) {
            //Should not happen: every observation is fetched by analysis entity
            //id from the same scope. Kept explicit rather than silently null.
            return {
                _analysis_entity_id: isBlank(aeId) ? null : parseInt(aeId),
                _dataset_id: null,
                _physical_sample_id: null,
                Action: "", Site: null, Sample: null, Dataset: null, Method: null,
            };
        }
        return {
            _analysis_entity_id: parseInt(e.analysis_entity_id),
            _dataset_id: isBlank(e.dataset_id) ? null : parseInt(e.dataset_id),
            _physical_sample_id: isBlank(e.physical_sample_id) ? null : parseInt(e.physical_sample_id),
            Action: "",
            Site: this._siteNameById(ctx, e.site_id),
            Sample: clean(e.sample_name),
            Dataset: clean(e.dataset_name),
            Method: clean(e.method_name),
        };
    }

    _buildAbundances(ctx) {
        const b = this._observationSheet("Abundances", "one row per sample x taxon",
            "Long format - the attribute axis (taxa) is open-ended, so it is never pivoted (design: 'When to pivot').");
        b.idCol("_abundance_id", { source: "public.tbl_abundances.abundance_id", table: "tbl_abundances", type: "integer" });
        b.idCol("_taxon_id", { source: "public.tbl_abundances.taxon_id", table: "tbl_abundances", type: "integer" });
        b.col("Taxon", { source: "public.tbl_abundances.taxon_id -> Taxa sheet", table: "tbl_abundances", type: "text" });
        b.col("Element", { source: "public.tbl_abundances.abundance_element_id", table: "tbl_abundances", type: "enum", vocab: "abundance_element" });
        b.col("Abundance", { source: "public.tbl_abundances.abundance", table: "tbl_abundances", type: "number" });
        b.col("Modifications", { source: "public.tbl_abundance_modifications", table: "tbl_abundance_modifications", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("Last updated", { source: "public.tbl_abundances.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });

        for (const a of ctx.source.abundances) {
            const taxon = ctx.source.taxa.get(String(a.taxon_id));
            b.addRow({
                ...this._entityRow(ctx, a.analysis_entity_id),
                _abundance_id: parseInt(a.abundance_id),
                _taxon_id: isBlank(a.taxon_id) ? null : parseInt(a.taxon_id),
                Taxon: taxon ? taxon.label : (isBlank(a.taxon_id) ? null : String(a.taxon_id)),
                Element: isBlank(a.abundance_element_id) ? null
                    : this._voc(ctx, "abundance_element", a.abundance_element_id,
                        clean(a.element_name) || a.abundance_element_id),
                Abundance: isBlank(a.abundance) ? null : Number(a.abundance),
                Modifications: (a.modifications || []).filter(Boolean).join("; ") || null,
                "Last updated": a.date_updated || null,
            });
        }
        return b;
    }

    _buildMeasurements(ctx) {
        const b = this._observationSheet("Measurements", "one row per measured value",
            "tbl_measured_values, keyed on the analysis entity that owns each value.");
        b.idCol("_measured_value_id", { source: "public.tbl_measured_values.measured_value_id", table: "tbl_measured_values", type: "integer" });
        b.col("Value", { source: "public.tbl_measured_values.measured_value", table: "tbl_measured_values", type: "number" });
        b.col("Last updated", { source: "public.tbl_measured_values.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });

        for (const m of ctx.source.measured) {
            b.addRow({
                ...this._entityRow(ctx, m.analysis_entity_id),
                _measured_value_id: parseInt(m.measured_value_id),
                Value: isBlank(m.measured_value) ? null : Number(m.measured_value),
                "Last updated": m.date_updated || null,
            });
        }
        return b;
    }

    /*
     * Ceramics has no value-class twin, unlike dendro.
     *
     * Measured against sead_staging: value classes exist for exactly two methods, 10
     * (Dendrochronology) and 175 (Ancient DNA), and none of the 11,076 analysis entities in
     * tbl_ceramics carries a single tbl_analysis_values row. So tbl_ceramics is the only home this
     * data has, and reading it is not a legacy shortcut - it is the whole source. If ceramics is
     * migrated into the value-class system later, this sheet becomes a D11 case like dendro and
     * should switch over; until then there is nothing to switch to.
     */
    _buildCeramics(ctx) {
        const b = this._observationSheet("Ceramics", "one row per analysis entity",
            "tbl_ceramics pivoted on tbl_ceramics_lookup. Values are stored as varchar in the database and are carried verbatim.");

        //Columns first, in lookup id order, so the layout is stable between
        //exports regardless of which rows happen to come back first.
        const lookups = new Map();
        for (const c of ctx.source.ceramics) {
            const id = String(c.ceramics_lookup_id);
            if (!lookups.has(id)) {
                lookups.set(id, {
                    id: parseInt(c.ceramics_lookup_id),
                    name: clean(c.lookup_name) || `lookup ${c.ceramics_lookup_id}`,
                    description: clean(c.lookup_description),
                });
            }
        }
        const colKeyFor = new Map();
        for (const l of [...lookups.values()].sort((a, b2) => a.id - b2.id)) {
            const colKey = l.name;
            colKeyFor.set(String(l.id), colKey);
            b.col(colKey, {
                title: colKey,
                source: `pivot:tbl_ceramics:ceramics_lookup_id=${l.id}`,
                table: "tbl_ceramics",
                group: COL_GROUP.PIVOT,
                type: "text",
                pivotType: l.name,
            });
        }

        const rows = new Map();
        for (const c of ctx.source.ceramics) {
            const key = String(c.analysis_entity_id);
            if (!rows.has(key)) rows.set(key, this._entityRow(ctx, c.analysis_entity_id));
            const row = rows.get(key);
            const colKey = colKeyFor.get(String(c.ceramics_lookup_id));
            const value = clean(c.measurement_value);
            row[colKey] = isBlank(row[colKey]) ? value : `${row[colKey]} | ${value}`;
        }
        for (const row of rows.values()) b.addRow(row);
        return b;
    }

    _buildIsotopes(ctx) {
        const b = this._observationSheet("Isotopes", "one row per isotope measurement",
            "tbl_isotopes with its measurement, standard, specifier and unit lookups.");
        b.idCol("_isotope_id", { source: "public.tbl_isotopes.isotope_id", table: "tbl_isotopes", type: "integer" });
        b.col("Isotope", { source: "public.tbl_isotope_types.designation", table: "tbl_isotopes", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("Value", { source: "public.tbl_isotopes.measurement_value", table: "tbl_isotopes", type: "number" });
        b.col("Unit", { source: "public.tbl_units.unit_abbrev", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("Specifier", { source: "public.tbl_isotope_value_specifiers.name", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("Standard", { source: "public.tbl_isotope_standards.isotope_ration", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("Last updated", { source: "public.tbl_isotopes.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });

        for (const i of ctx.source.isotopes) {
            const n = Number(i.measurement_value);
            b.addRow({
                ...this._entityRow(ctx, i.analysis_entity_id),
                _isotope_id: parseInt(i.isotope_id),
                Isotope: clean(i.isotope_type_name),
                //measurement_value is text in the database; keep it as text when
                //it is not a clean number rather than exporting NaN.
                Value: isBlank(i.measurement_value) ? null : (Number.isFinite(n) ? n : clean(i.measurement_value)),
                Unit: clean(i.unit_abbrev) || clean(i.unit_name),
                Specifier: clean(i.specifier_name),
                Standard: clean(i.isotope_ration) || clean(i.international_scale),
                "Last updated": i.date_updated || null,
            });
        }
        return b;
    }

    /**
     * Pivot value-class observations into a wide sheet: one row per analysis
     * entity, one column per value class.
     *
     * Value classes are a closed set per method, so the attribute axis is
     * bounded and pivoting is right (design, "When to pivot"). Each column is
     * typed from its class, and carries the *typed* value from the subtable
     * behind it rather than the text rendering wherever one exists - which is
     * what makes these columns writable at all (design F1).
     *
     * A "(qualifier)" column is emitted beside a class only when some value in
     * this export actually carries one, so the 1,760 integer and 43 boolean
     * qualifiers stop being dropped without widening every other sheet.
     */
    _buildValueClassSheet(ctx, name, grain, note, predicate) {
        const b = this._observationSheet(name, grain, note);
        const values = ctx.source.analysisValues.filter(predicate);

        //Column properties are decided from every value of a class in this
        //export, not from whichever row arrives first: one typed row makes the
        //class writable, one qualifier gives it a qualifier column.
        const classes = new Map();
        for (const v of values) {
            const id = String(v.value_class_id);
            if (!classes.has(id)) {
                classes.set(id, {
                    id: parseInt(v.value_class_id),
                    name: clean(v.class_name) || `class ${v.value_class_id}`,
                    description: clean(v.class_description),
                    valueTypeId: v.value_type_id,
                    unit: clean(v.value_type_unit_abbrev),
                    typed: false,
                    qualifier: false,
                    kinds: new Set(),
                });
            }
            const c = classes.get(id);
            if (v.has_typed_row) c.typed = true;
            if (v.resolved_qualifier) c.qualifier = true;
            c.kinds.add(v.resolved_from);
        }

        const colKeyFor = new Map();
        for (const c of [...classes.values()].sort((a, b2) => a.id - b2.id)) {
            const colKey = `Class: ${c.name}${c.unit ? ` (${c.unit})` : ""}`;
            colKeyFor.set(String(c.id), colKey);
            const kind = c.kinds.size === 1 ? [...c.kinds][0] : "mixed";
            b.col(colKey, {
                title: colKey,
                source: `pivot:tbl_analysis_values:value_class_id=${c.id}`,
                table: "tbl_analysis_values",
                group: COL_GROUP.PIVOT,
                type: this._valueClassCellType(kind),
                //A class with no typed row anywhere is carried by its text
                //rendering alone, which cannot be regenerated (design Q2), so it
                //stays reference-grade. One with a typed subtable round-trips.
                roundtrip: c.typed ? ROUNDTRIP.EDITABLE : ROUNDTRIP.REFERENCE,
                locked: !c.typed,
                vocab: kind === "categorical" ? `value_type_${c.valueTypeId}` : null,
                pivotType: c.name,
            });
            if (c.qualifier) {
                const qKey = `${colKey} (qualifier)`;
                b.col(qKey, {
                    title: qKey,
                    source: `pivot:tbl_analysis_values:value_class_id=${c.id}:qualifier`,
                    table: "tbl_analysis_values",
                    group: COL_GROUP.PIVOT,
                    type: "text",
                    pivotType: c.name,
                });
            }
        }

        const rows = new Map();
        for (const v of values) {
            const key = String(v.analysis_entity_id);
            if (!rows.has(key)) rows.set(key, this._entityRow(ctx, v.analysis_entity_id));
            const row = rows.get(key);
            const colKey = colKeyFor.get(String(v.value_class_id));

            //Measured across sead_staging a class is single-valued per analysis
            //entity, but the format must not silently drop a second value if
            //that ever stops being true.
            row[colKey] = isBlank(row[colKey]) ? v.resolved_value
                : `${row[colKey]} | ${v.resolved_value}`;

            if (v.resolved_qualifier) row[`${colKey} (qualifier)`] = v.resolved_qualifier;

            if (v.resolved_from === "categorical" && !isBlank(v.categorical_item_id)) {
                this._voc(ctx, `value_type_${v.value_type_id}`, v.categorical_item_id,
                    clean(v.categorical_item_name) || v.categorical_item_id,
                    clean(v.categorical_item_description));
            }
        }
        for (const row of rows.values()) b.addRow(row);
        return b;
    }

    _valueClassCellType(kind) {
        switch (kind) {
            case "integer": return "integer";
            case "numerical": return "number";
            case "categorical": return "enum";
            default: return "text";
        }
    }

    _buildDendrochronology(ctx) {
        return this._buildValueClassSheet(
            ctx, "Dendrochronology", "one row per analysis entity",
            `tbl_analysis_values for method ${DENDRO_METHOD_ID}'s value classes (D11: the value-class system is authoritative over tbl_dendro).`,
            v => parseInt(v.class_method_id) === DENDRO_METHOD_ID);
    }

    _buildAnalysisValues(ctx) {
        return this._buildValueClassSheet(
            ctx, "Analysis Values", "one row per analysis entity",
            "tbl_analysis_values joined to its typed subtables, pivoted per value class. Dendrochronology has its own sheet.",
            v => parseInt(v.class_method_id) !== DENDRO_METHOD_ID);
    }

    /*
     * Dating: the non-value-class dating tables, collapsed under a "Dating type" discriminator that
     * names the table a row came from, so every column keeps a real binding.
     *
     * The previous version read data_groups of type "dating", which DatingModule has already
     * flattened into generic {key, value} pairs for display. That made the sheet readable but
     * unwritable - the columns bound to "discriminator" and "resolved dating value" rather than to
     * any column, so nothing could be resolved on the way back in. These tables are queried
     * directly instead. Columns not applicable to a given source table are left blank.
     *
     * Three sources, not five: tbl_dendro_dates and tbl_analysis_dating_ranges were dropped once
     * the Dendrochronology sheet started carrying their value-class equivalents, which subsume
     * them entirely (D11). Both were being exported twice.
     */
    _buildDating(ctx) {
        const b = new SheetBuilder("Dating", "B", "one row per date determination", {
            note: "tbl_geochronology, tbl_relative_dates and tbl_analysis_entity_ages under a Dating type discriminator. Dendro dates live on the Dendrochronology sheet as value classes (D11).",
        });
        b.idCol("_dating_id", { source: "primary key of the table named by Dating type", type: "integer" });
        b.idCol("_analysis_entity_id", { source: "public.tbl_analysis_entities.analysis_entity_id", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample", { source: "context:tbl_physical_samples.sample_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Dating type", { source: "discriminator: source table", type: "enum", vocab: "dating_type", roundtrip: ROUNDTRIP.REFERENCE, locked: true });
        b.col("Lab", { source: "public.tbl_dating_labs.lab_name", roundtrip: ROUNDTRIP.REFERENCE, locked: true });
        b.col("Lab number", { source: "public.tbl_geochronology.lab_number", type: "text" });
        b.col("Age", { source: "public.tbl_geochronology.age / tbl_analysis_entity_ages.age", type: "number" });
        b.col("Error older", { source: "public.tbl_geochronology.error_older", type: "number" });
        b.col("Error younger", { source: "public.tbl_geochronology.error_younger", type: "number" });
        b.col("Delta 13C", { source: "public.tbl_geochronology.delta_13c", type: "number" });
        b.col("Age older", { source: "public.tbl_analysis_entity_ages.age_older", type: "number" });
        b.col("Age younger", { source: "public.tbl_analysis_entity_ages.age_younger", type: "number" });
        b.col("Relative age", { source: "public.tbl_relative_ages.relative_age_name", type: "text" });
        b.col("Age type", { source: "public.tbl_age_types.age_type", type: "enum", vocab: "age_type" });
        b.col("Season", { source: "public.tbl_seasons.season_name", type: "enum", vocab: "season" });
        b.col("Chronology", { source: "public.tbl_chronologies.chronology_name", type: "text" });
        b.col("Dating uncertainty", { source: "public.tbl_dating_uncertainty.uncertainty", type: "enum", vocab: "dating_uncertainty" });
        b.col("Notes", { source: "public.tbl_geochronology.notes / tbl_relative_dates.notes", type: "text" });
        b.col("Last updated", { source: "date_updated of the table named by Dating type", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });

        const ctxFor = this._analysisEntityContext(ctx);
        const base = (aeId, type) => {
            const c = ctxFor.get(String(aeId)) || {};
            this._voc(ctx, "dating_type", type, type);
            return {
                _analysis_entity_id: isBlank(aeId) ? null : parseInt(aeId),
                Action: "",
                Site: c.siteName || null,
                Sample: c.sampleName || null,
                "Dating type": type,
            };
        };

        for (const g of ctx.extra.geochronology || []) {
            b.addRow({
                ...base(g.analysis_entity_id, "geochronology"),
                _dating_id: parseInt(g.geochron_id),
                Lab: clean(g.lab_name),
                "Lab number": clean(g.lab_number),
                Age: this._num(g.age),
                "Error older": this._num(g.error_older),
                "Error younger": this._num(g.error_younger),
                "Delta 13C": this._num(g.delta_13c),
                "Dating uncertainty": this._voc(ctx, "dating_uncertainty", g.dating_uncertainty_id, g.uncertainty),
                Notes: clean(g.notes),
                "Last updated": g.date_updated || null,
            });
        }

        for (const r of ctx.extra.relative_dates || []) {
            b.addRow({
                ...base(r.analysis_entity_id, "relative_date"),
                _dating_id: parseInt(r.relative_date_id),
                "Relative age": clean(r.relative_age_name),
                "Age older": this._num(r.cal_age_older ?? r.c14_age_older),
                "Age younger": this._num(r.cal_age_younger ?? r.c14_age_younger),
                "Dating uncertainty": this._voc(ctx, "dating_uncertainty", r.dating_uncertainty_id, r.uncertainty),
                Notes: clean(r.notes),
                "Last updated": r.date_updated || null,
            });
        }

        /*
         * tbl_dendro_dates is deliberately NOT emitted here.
         *
         * D11: the value-class system is authoritative over the legacy dendro
         * tables, and the measurement backs it - all 7,318 analysis entities in
         * tbl_dendro_dates also carry value-class dating ranges, which cover 138
         * more besides. Every one of those ranges belongs to method 10, so they
         * are already pivoted onto the Dendrochronology sheet as
         * "Estimated felling year", "Outermost tree-ring date" and the rest.
         *
         * Emitting them here as well put the same date on two sheets with two
         * different primary keys, which is exactly the duplication D11 exists to
         * prevent: an editor could change one and not the other, and the
         * importer would have no way to say which was meant.
         */

        for (const a of ctx.extra.entity_ages || []) {
            b.addRow({
                ...base(a.analysis_entity_id, "entity_age"),
                _dating_id: parseInt(a.analysis_entity_age_id),
                Age: this._num(a.age),
                "Age older": this._num(a.age_older),
                "Age younger": this._num(a.age_younger),
                Chronology: clean(a.chronology_name),
                "Last updated": a.date_updated || null,
            });
        }

        /*
         * tbl_analysis_dating_ranges is likewise not emitted here. Measured
         * against sead_staging, every one of its 7,775 rows belongs to a
         * method 10 value class, so the Dendrochronology sheet already carries
         * all of them in their own class columns, with the range formatted and
         * its qualifiers kept. A dating range is a value-class value; the
         * Dating sheet is for the dating tables that are not.
         *
         * If a non-dendro method ever acquires dating ranges, they will show up
         * as unshipped analysis entities in manifest.coverage.analysis_entities
         * rather than being silently dropped.
         */

        return b;
    }

    /*
     * tbl_analysis_entity_prep_methods - 34,477 rows that reached no sheet before. The JSON API
     * carries prep method ids on the analysis entity but the exporter never surfaced them.
     */
    _buildPrepMethods(ctx) {
        const b = new SheetBuilder("Prep Methods", "B", "one row per analysis entity x preparation method", {
            note: "Sample preparation applied before analysis.",
        });
        b.idCol("_analysis_entity_prep_method_id", { source: "public.tbl_analysis_entity_prep_methods.analysis_entity_prep_method_id", type: "integer" });
        b.idCol("_analysis_entity_id", { source: "public.tbl_analysis_entity_prep_methods.analysis_entity_id", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample", { source: "context:tbl_physical_samples.sample_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Preparation method", { source: "public.tbl_analysis_entity_prep_methods.method_id", type: "enum", vocab: "prep_method" });
        b.col("Last updated", { source: "public.tbl_analysis_entity_prep_methods.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });

        const ctxFor = this._analysisEntityContext(ctx);
        for (const p of ctx.extra.prep_methods || []) {
            const c = ctxFor.get(String(p.analysis_entity_id)) || {};
            b.addRow({
                _analysis_entity_prep_method_id: parseInt(p.analysis_entity_prep_method_id),
                _analysis_entity_id: parseInt(p.analysis_entity_id),
                Action: "",
                Site: c.siteName || null,
                Sample: c.sampleName || null,
                "Preparation method": this._voc(ctx, "prep_method", p.method_id, p.method_name, p.method_description),
                "Last updated": p.date_updated || null,
            });
        }
        return b;
    }

    /*
     * tbl_abundance_ident_levels - how confidently a taxon was identified. Kept on its own sheet
     * rather than as a column on Abundances because an abundance can carry several levels.
     */
    _buildIdentificationLevels(ctx) {
        const b = new SheetBuilder("Identification Levels", "B", "one row per abundance x identification level", {
            note: "Confidence of the taxonomic identification behind an abundance row.",
        });
        b.idCol("_abundance_ident_level_id", { source: "public.tbl_abundance_ident_levels.abundance_ident_level_id", type: "integer" });
        b.idCol("_abundance_id", { source: "public.tbl_abundance_ident_levels.abundance_id", type: "integer" });
        b.idCol("_analysis_entity_id", { source: "public.tbl_abundances.analysis_entity_id", type: "integer" });
        b.actionCol();
        b.col("Site", { source: "context:tbl_sites.site_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Sample", { source: "context:tbl_physical_samples.sample_name", roundtrip: ROUNDTRIP.DERIVED, group: COL_GROUP.CONTEXT, locked: true });
        b.col("Identification level", { source: "public.tbl_abundance_ident_levels.identification_level_id", type: "enum", vocab: "identification_level" });
        b.col("Notes", { source: "public.tbl_identification_levels.notes", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("Last updated", { source: "public.tbl_abundance_ident_levels.date_updated", roundtrip: ROUNDTRIP.REFERENCE, group: COL_GROUP.UPDATED, locked: true, type: "date" });

        const ctxFor = this._analysisEntityContext(ctx);
        for (const il of ctx.extra.ident_levels || []) {
            const c = ctxFor.get(String(il.analysis_entity_id)) || {};
            b.addRow({
                _abundance_ident_level_id: parseInt(il.abundance_ident_level_id),
                _abundance_id: parseInt(il.abundance_id),
                _analysis_entity_id: parseInt(il.analysis_entity_id),
                Action: "",
                Site: c.siteName || null,
                Sample: c.sampleName || null,
                "Identification level": this._voc(ctx, "identification_level", il.identification_level_id,
                    il.identification_level_name || il.identification_level_abbrev),
                Notes: clean(il.notes),
                "Last updated": il.date_updated || null,
            });
        }
        return b;
    }

    // ---------------------------------------------------------------- Tier C

    _buildTaxa(ctx) {
        const b = new SheetBuilder("Taxa", "C", "one row per taxon", { note: "Reference sheet. Not editable through SDF (D7)." });
        b.idCol("_taxon_id", { source: "public.tbl_taxa_tree_master.taxon_id", type: "integer" });
        for (const c of ["Genus", "Family", "Order", "Species", "Author", "Common names"]) {
            b.col(c, { source: `public.tbl_taxa_tree_master (${c})`, roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        }
        const seen = new Set();
        for (const site of ctx.sites) {
            for (const t of (site.lookup_tables && site.lookup_tables.taxa) || []) {
                if (seen.has(String(t.taxon_id))) continue;
                seen.add(String(t.taxon_id));
                b.addRow({
                    _taxon_id: parseInt(t.taxon_id),
                    Genus: clean(t.genus && (t.genus.genus_name || t.genus.name)),
                    Family: clean(t.family && (t.family.family_name || t.family.name)),
                    Order: clean(t.order && (t.order.order_name || t.order.name)),
                    Species: clean(t.species),
                    Author: clean(t.author && (t.author.author_name || t.author.name)),
                    "Common names": ((t.common_names || []).map(c => c.common_name || c.name).filter(Boolean).join("; ")) || null,
                });
            }
        }
        return b;
    }

    _buildBibliography(ctx) {
        const b = new SheetBuilder("Bibliography", "C", "one row per reference", { note: "Reference sheet." });
        b.idCol("_biblio_id", { source: "public.tbl_biblio.biblio_id", type: "integer" });
        for (const c of ["Full reference", "Title", "Authors", "Year", "DOI", "ISBN", "URL"]) {
            b.col(c, { source: `public.tbl_biblio (${c})`, roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        }
        const seen = new Set();
        for (const site of ctx.sites) {
            for (const r of (site.lookup_tables && site.lookup_tables.biblio) || []) {
                if (seen.has(String(r.biblio_id))) continue;
                seen.add(String(r.biblio_id));
                b.addRow({
                    _biblio_id: parseInt(r.biblio_id),
                    "Full reference": this._biblioText(r),
                    Title: clean(r.title),
                    Authors: clean(r.authors),
                    Year: clean(r.year),
                    DOI: clean(r.doi),
                    ISBN: clean(r.isbn),
                    URL: clean(r.url),
                });
            }
        }
        return b;
    }

    _buildMethods(ctx) {
        const b = new SheetBuilder("Methods", "C", "one row per method", { note: "Reference sheet." });
        b.idCol("_method_id", { source: "public.tbl_methods.method_id", type: "integer" });
        for (const c of ["Method name", "Abbreviation", "Description", "Method group", "Unit"]) {
            b.col(c, { source: `public.tbl_methods (${c})`, roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        }
        const seen = new Set();
        for (const site of ctx.sites) {
            const lt = site.lookup_tables || {};
            for (const m of [...(lt.methods || []), ...(lt.prep_methods || [])]) {
                if (seen.has(String(m.method_id))) continue;
                seen.add(String(m.method_id));
                this._voc(ctx, "method", m.method_id, m.method_name || m.method_id, m.description);
                b.addRow({
                    _method_id: parseInt(m.method_id),
                    "Method name": clean(m.method_name),
                    Abbreviation: clean(m.method_abbrev_or_alt_name),
                    Description: clean(m.description),
                    "Method group": clean(m.method_group_id),
                    Unit: clean(m.unit_id),
                });
            }
        }
        return b;
    }

    _buildVocabularies(ctx) {
        const b = new SheetBuilder("Vocabularies", "C", "one row per term", {
            note: "list / code / label / description. Feeds every dropdown. Only the terms this bundle uses are present (D7).",
        });
        b.col("list", { source: "vocabulary list name", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("code", { source: "term id in its source table", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("label", { source: "term label", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });
        b.col("description", { source: "term description", roundtrip: ROUNDTRIP.REFERENCE, locked: true, type: "text" });

        //the action pseudo-vocabulary, so an importer can validate the column
        for (const a of ["", "delete"]) this._voc(ctx, "action", a || "(blank)", a || "(blank)");

        for (const [list, terms] of [...ctx.vocab.entries()].sort()) {
            for (const t of [...terms.values()].sort((a, b) => String(a.label).localeCompare(String(b.label)))) {
                b.addRow({ list, code: t.code, label: t.label, description: t.description });
            }
        }
        return b;
    }

    // ---------------------------------------------------------------- manifest

    _buildManifest(ctx, finalizedSheets) {
        const columns = [];
        for (const sheet of finalizedSheets) {
            for (const c of sheet.columns) {
                columns.push({
                    sheet: sheet.name,
                    key: c.key,
                    title: c.title,
                    source: c.source,
                    roundtrip: c.roundtrip,
                    hidden: c.hidden,
                    locked: c.locked,
                    type: c.type,
                    vocab: c.vocab || undefined,
                    pivot_type: c.pivotType || undefined,
                });
            }
        }

        const coveredTables = new Set();
        for (const s of finalizedSheets) for (const t of s.source_tables) coveredTables.add(t);

        const gaps = KNOWN_API_GAPS.map(g => ({
            table: g.table,
            status: g.handled ? "covered by SDF (queried directly)" : "not yet covered — pending Phase 2",
            sheet: g.sheet,
        }));

        const sheetsInventory = finalizedSheets.map(s => ({
            name: s.name,
            tier: s.tier,
            grain: s.grain,
            row_count: s.rows.length,
            column_count: s.columns.length,
            source_tables: s.source_tables,
        }));

        const body = {
            sdf_version: SDF_VERSION,
            exporter_build: `${this.app.appName}-${this.app.appVersion}`,
            database_release: process.env.SEAD_DATABASE_RELEASE || "unknown",
            source_database: process.env.POSTGRES_DATABASE || "unknown",
            exported_at: new Date().toISOString(),
            profile: ctx.profile,
            site_ids: ctx.sites.map(s => parseInt(s.site_id)),
            sheets: sheetsInventory,
            columns,
            coverage: {
                covered_source_tables: [...coveredTables].sort(),
                api_gap_tables: gaps,
                analysis_entities: this._auditEntities(ctx, finalizedSheets),
                notes: [
                    "Tier B sheets are built from tbl_analysis_entities and the value tables directly, scoped by the site's own sample groups. No method allowlist is consulted and site.data_groups is not read (design F2, F3).",
                    "Typed analysis subtables are joined; each value carries its typed value where one exists, and the text rendering otherwise. Which one was used is recorded per value class by the column's roundtrip flag (design F1, Q2).",
                    "Boolean value classes keep their Swedish text rendering ('Ja'/'Nej') rather than the typed boolean, by decision; the typed value exists in tbl_analysis_boolean_values.",
                    "_Raw appendix for remaining uncovered owned tables (D6) is not implemented in this profile.",
                ],
            },
        };
        body.checksum = checksum({ sheets: finalizedSheets, site_ids: body.site_ids }, crypto);
        return body;
    }

    /**
     * The completeness self-audit the design asks for in Phase 0.
     *
     * Every analysis entity the site owns must reach at least one observation
     * sheet. One that reaches none means the export is quietly short - a method
     * whose data lives in a table SDF does not read yet. Reporting it by method
     * is what turns F2 from an invisible defect into a visible one.
     *
     * This is a report, not a gate: it is recorded in the manifest so an
     * importer or a curator can see it, rather than failing an export that is
     * still useful for everything it did cover.
     */
    _auditEntities(ctx, finalizedSheets) {
        const src = ctx.source;

        //Audit what actually shipped, by reading the _analysis_entity_id column
        //out of every finalized sheet, rather than what the routing predicted.
        //Sheets built outside SdfSource - Dating, Prep Methods, Identification
        //Levels - then count automatically, and so will any sheet added later.
        const shipped = new Set();
        const perSheet = {};
        for (const sheet of finalizedSheets) {
            const idx = sheet.columns.findIndex(c => c.key === "_analysis_entity_id");
            if (idx < 0) continue;
            const here = new Set();
            for (const row of sheet.rows) {
                const v = row[idx];
                if (isBlank(v)) continue;
                here.add(String(v));
                shipped.add(String(v));
            }
            if (here.size) perSheet[sheet.name] = here.size;
        }

        //group the misses by method so the report names a cause, not just a count
        const byMethod = new Map();
        const missed = [];
        for (const e of src.entities) {
            if (shipped.has(String(e.analysis_entity_id))) continue;
            missed.push(e);
            const key = `${e.method_id}`;
            if (!byMethod.has(key)) {
                byMethod.set(key, {
                    method_id: isBlank(e.method_id) ? null : parseInt(e.method_id),
                    method_name: clean(e.method_name),
                    data_type_name: clean(e.data_type_name),
                    analysis_entities: 0,
                    dataset_ids: new Set(),
                });
            }
            const m = byMethod.get(key);
            m.analysis_entities++;
            if (!isBlank(e.dataset_id)) m.dataset_ids.add(parseInt(e.dataset_id));
        }

        return {
            total: src.entities.length,
            shipped: shipped.size,
            missing: missed.length,
            complete: missed.length === 0,
            entities_per_sheet: perSheet,
            //An analysis entity that reaches no sheet holds no observation SDF
            //knows how to read. Usually that means the entity is genuinely empty
            //(the orphan datasets the design records), but it is also how a
            //method whose table SDF does not cover would announce itself.
            missing_by_method: [...byMethod.values()]
                .map(m => ({
                    method_id: m.method_id,
                    method_name: m.method_name,
                    data_type_name: m.data_type_name,
                    analysis_entities: m.analysis_entities,
                    dataset_count: m.dataset_ids.size,
                }))
                .sort((a, b) => b.analysis_entities - a.analysis_entities),
        };
    }

    // ---------------------------------------------------------------- resolvers

    _absorbByType(ctx, b, row, rows, { prefix, table, typeName, value }) {
        const byType = new Map();
        for (const r of rows) {
            const tn = clean(typeName(r)) || "(unspecified)";
            if (!byType.has(tn)) byType.set(tn, []);
            byType.get(tn).push(clean(value(r)));
        }
        for (const [tn, vals] of byType) {
            const colKey = `${prefix}: ${tn}`;
            b.col(colKey, {
                title: colKey,
                source: `pivot:${table}:${tn}`,
                table,
                group: COL_GROUP.PIVOT,
                type: "text",
                pivotType: tn,
            });
            //design: report where a "single-valued" satellite turns out not to be
            row[colKey] = vals.filter(v => v !== null).map(String).join(" | ");
        }
    }

    _absorbCoordinates(ctx, b, row, coords, table) {
        for (const c of coords) {
            const dimName = c.dimension_id != null ? `dim_${c.dimension_id}` : "coord";
            const colKey = `Coordinate: ${dimName}`;
            b.col(colKey, { title: colKey, source: `pivot:${table}:${dimName}`, table, group: COL_GROUP.PIVOT, type: "number", pivotType: dimName });
            row[colKey] = isBlank(c.measurement) ? null : Number(c.measurement);
        }
    }

    _pivotType(ctx, list, arr, id, labelFn, descFn) {
        //sample-group sampling_context comes back as a one-element array
        if (Array.isArray(arr) && arr.length) {
            const o = arr[0];
            return this._voc(ctx, list, id ?? labelFn(o), labelFn(o), descFn ? descFn(o) : null);
        }
        return clean(id);
    }

    _methodLabel(site, methodId) {
        if (isBlank(methodId)) return null;
        const lt = site.lookup_tables || {};
        const m = [...(lt.methods || []), ...(lt.prep_methods || [])].find(x => String(x.method_id) === String(methodId));
        return m ? clean(m.method_name) : null;
    }

    _dimensionLabel(site, dimId) {
        if (isBlank(dimId)) return null;
        const d = ((site.lookup_tables || {}).dimensions || []).find(x => String(x.dimension_id) === String(dimId));
        return d ? clean(d.dimension_name) : null;
    }

    _biblioLabel(site, biblioId) {
        if (isBlank(biblioId)) return null;
        const r = ((site.lookup_tables || {}).biblio || []).find(x => String(x.biblio_id) === String(biblioId));
        return r ? this._biblioText(r) : String(biblioId);
    }

    _taxonLabel(site, taxonId) {
        if (isBlank(taxonId)) return null;
        const t = ((site.lookup_tables || {}).taxa || []).find(x => String(x.taxon_id) === String(taxonId));
        if (!t) return String(taxonId);
        const genus = t.genus && (t.genus.genus_name || t.genus.name);
        return [clean(genus), clean(t.species)].filter(Boolean).join(" ") || String(taxonId);
    }

    _biblioText(r) {
        return clean(r.full_reference)
            || [clean(r.authors), clean(r.year), clean(r.title)].filter(Boolean).join(". ")
            || clean(r.title)
            || (isBlank(r.biblio_id) ? null : `biblio ${r.biblio_id}`);
    }
}
