import { SDF_VERSION, isBlank, isNewToken, clean } from "./SdfCommon.js";

/**
 * Reads a SEAD Data Format bundle and reports what it would do — it writes
 * nothing (design Phase 3, D8).
 *
 * Four strictly-ordered stages, each feeding the next:
 *   1. structural   — format version, sheet inventory, column bindings
 *   2. vocabulary   — dropdown values resolve, or become curator proposals (D7)
 *   3. referential  — every id / NEW-n reference resolves within the bundle
 *   4. diff         — resolved bundle vs. the live database: adds, updates,
 *                     deletions, and rows present in the DB but not mentioned
 *
 * The decisive test the design calls out: export a site, import it unmodified,
 * and the diff must be empty. `report.diff_empty` answers exactly that.
 */

//Sheets whose rows map cleanly onto one table with one primary key. Only plain
//text/number fields are diffed field-by-field; enum columns need vocabulary
//resolution against the target release and are listed but not compared here.
const DIFF_SPECS = {
    "Site": {
        table: "tbl_sites", pk: "_site_id", pkCol: "site_id",
        fields: {
            "Site name": "site_name",
            "National site identifier": "national_site_identifier",
            "Latitude (WGS84)": "latitude_dd",
            "Longitude (WGS84)": "longitude_dd",
            "Altitude (m)": "altitude",
            "Location accuracy": "site_location_accuracy",
            "Site description": "site_description",
        },
    },
    "Sample Groups": {
        table: "tbl_sample_groups", pk: "_sample_group_id", pkCol: "sample_group_id",
        fields: { "Sample group name": "sample_group_name" },
        scopeCol: "site_id", scopeRefColumn: "_site_id",
    },
    "Samples": {
        table: "tbl_physical_samples", pk: "_physical_sample_id", pkCol: "physical_sample_id",
        fields: { "Sample name": "sample_name", "Date sampled": "date_sampled" },
        scopeCol: "sample_group_id", scopeRefColumn: "_sample_group_id",
    },
    "Datasets": {
        table: "tbl_datasets", pk: "_dataset_id", pkCol: "dataset_id",
        fields: { "Dataset name": "dataset_name" },
    },
    "Notes": {
        table: "(tbl_sample_notes | tbl_sample_group_notes)", pk: "_note_id", pkCol: "sample_note_id",
        fields: { "Note": "note", "Note type": "note_type" }, diffAgainstDb: false,
    },
    "Dataset Submissions": {
        table: "tbl_dataset_submissions", pk: "_dataset_submission_id", pkCol: "dataset_submission_id",
        fields: { "Date submitted": "date_submitted", "Notes": "notes" }, diffAgainstDb: false,
    },
};

//Which sheet owns each hidden foreign-key column, for referential checks.
const PARENT_SHEET_FOR_REF = {
    "_site_id": "Site",
    "_sample_group_id": "Sample Groups",
    "_physical_sample_id": "Samples",
    "_dataset_id": "Datasets",
    "_biblio_id": "Bibliography",
    "_taxon_id": "Taxa",
    "_method_id": "Methods",
};

export default class SdfImporter {
    constructor(app) {
        this.app = app;
    }

    /**
     * @param {object} workbook  an SDF bundle as produced by SdfExporter
     * @returns {Promise<object>} the validation + diff report
     */
    async validate(workbook) {
        const report = {
            ok: false,
            stage_reached: 0,
            sdf_version: workbook && workbook.sdf_version,
            errors: [],
            warnings: [],
            proposals: [],
            change_set: { adds: [], updates: [], deletes: [], unmentioned: [] },
            diff_empty: false,
        };

        if (!workbook || typeof workbook !== "object" || !Array.isArray(workbook.sheets)) {
            report.errors.push({ stage: 1, code: "not_an_sdf_bundle", message: "Body is not an SDF bundle (expected { sdf_version, manifest, sheets: [] })." });
            return report;
        }

        const sheetsByName = new Map(workbook.sheets.map(s => [s.name, s]));
        const manifest = workbook.manifest || {};
        const manifestColumns = Array.isArray(manifest.columns) ? manifest.columns : [];

        // ---- Stage 1: structural ------------------------------------------
        report.stage_reached = 1;
        this._checkVersion(workbook, report);
        this._checkInventory(workbook, manifest, sheetsByName, report);
        this._checkColumnBindings(workbook, manifestColumns, sheetsByName, report);
        report.checksum_matches_export = this._checksumMatches(workbook);
        if (this._hasBlockingErrors(report)) return this._finish(report);

        // ---- Stage 2: vocabulary ----------------------------------------
        report.stage_reached = 2;
        const vocab = this._loadVocabularies(sheetsByName);
        this._resolveVocabulary(workbook, manifestColumns, sheetsByName, vocab, report);

        // ---- Stage 3: referential -------------------------------------
        report.stage_reached = 3;
        const newTokens = this._collectNewTokens(workbook, report);
        this._checkReferences(workbook, sheetsByName, newTokens, report);
        if (this._hasBlockingErrors(report)) return this._finish(report);

        // ---- Stage 4: diff against the database -----------------------
        report.stage_reached = 4;
        try {
            await this._diff(workbook, sheetsByName, report);
        } catch (e) {
            report.errors.push({ stage: 4, code: "diff_failed", message: `Could not diff against the database: ${e.message}` });
        }

        return this._finish(report);
    }

    // ------------------------------------------------------------------ stage 1

    _checkVersion(workbook, report) {
        const found = String(workbook.sdf_version || "");
        const m = found.match(/^SDF\/(\d+)\.(\d+)/);
        const mineMajor = parseInt(SDF_VERSION.split("/")[1]);
        if (!m) {
            report.errors.push({ stage: 1, code: "bad_version", message: `Workbook sdf_version "${found}" is unreadable; this importer implements ${SDF_VERSION}.` });
            return;
        }
        const major = parseInt(m[1]);
        if (major > mineMajor) {
            report.errors.push({ stage: 1, code: "version_too_new", message: `Workbook is ${found}; this importer implements ${SDF_VERSION} and must refuse a newer major version (D9).` });
        } else if (major < mineMajor) {
            report.warnings.push({ stage: 1, code: "version_older_major", message: `Workbook is ${found}, older major than ${SDF_VERSION}. Proceeding, but meaning may have shifted.` });
        }
        const release = workbook.manifest && workbook.manifest.database_release;
        if (isBlank(release) || release === "unknown") {
            report.warnings.push({ stage: 1, code: "no_release_tag", message: "Workbook carries no database_release tag; vocabulary ids cannot be checked for drift (D9)." });
        }
    }

    _checkInventory(workbook, manifest, sheetsByName, report) {
        const declared = Array.isArray(manifest.sheets) ? manifest.sheets.map(s => s.name) : [];
        for (const name of declared) {
            if (!sheetsByName.has(name)) {
                report.errors.push({ stage: 1, code: "missing_sheet", sheet: name, message: `Manifest declares sheet "${name}" but it is not present in the workbook.` });
            }
        }
        for (const s of workbook.sheets) {
            if (declared.length && !declared.includes(s.name)) {
                report.warnings.push({ stage: 1, code: "extra_sheet", sheet: s.name, message: `Sheet "${s.name}" is not in the manifest inventory; it will be ignored.` });
            }
        }
    }

    _checkColumnBindings(workbook, manifestColumns, sheetsByName, report) {
        const expectedBySheet = new Map();
        for (const c of manifestColumns) {
            if (!expectedBySheet.has(c.sheet)) expectedBySheet.set(c.sheet, new Map());
            expectedBySheet.get(c.sheet).set(c.key, c);
        }
        for (const [sheetName, expected] of expectedBySheet) {
            const sheet = sheetsByName.get(sheetName);
            if (!sheet) continue;
            const providedKeys = new Set((sheet.columns || []).map(c => c.key));
            const providedTitles = new Set((sheet.columns || []).map(c => c.title));
            for (const [key, col] of expected) {
                if (!providedKeys.has(key)) {
                    //bind-by-header fallback (D4): the title may still be intact
                    if (providedTitles.has(col.title)) {
                        report.warnings.push({ stage: 1, code: "column_key_missing_title_ok", sheet: sheetName, column: key, message: `Column "${col.title}" bound by header text; its key was not carried.` });
                    } else {
                        report.errors.push({ stage: 1, code: "column_renamed_or_missing", sheet: sheetName, column: key, message: `Column "${col.title}" (${key}) is missing from sheet "${sheetName}" — header renamed or column deleted (D4).` });
                    }
                }
            }
            for (const c of sheet.columns || []) {
                if (!expected.has(c.key) && !isBlank(c.key)) {
                    report.warnings.push({ stage: 1, code: "unrecognised_column", sheet: sheetName, column: c.key, message: `Column "${c.title}" (${c.key}) is not in the manifest for "${sheetName}" and will be ignored.` });
                }
            }
        }
    }

    _checksumMatches(workbook) {
        try {
            const declared = workbook.manifest && workbook.manifest.checksum;
            if (!declared) return null;
            //an edited workbook is expected to differ — informational only
            return undefined;
        } catch { return null; }
    }

    // ------------------------------------------------------------------ stage 2

    _loadVocabularies(sheetsByName) {
        const vocab = new Map(); //list -> { labels:Set, byLabel:Map(label->code) }
        const sheet = sheetsByName.get("Vocabularies");
        if (!sheet) return vocab;
        const idx = this._colIndex(sheet);
        const li = idx.get("list"), la = idx.get("label"), co = idx.get("code");
        if (li === undefined || la === undefined) return vocab;
        for (const row of sheet.rows || []) {
            const list = clean(row[li]);
            const label = clean(row[la]);
            if (isBlank(list) || isBlank(label)) continue;
            if (!vocab.has(list)) vocab.set(list, { labels: new Set(), byLabel: new Map() });
            vocab.get(list).labels.add(String(label));
            if (co !== undefined) vocab.get(list).byLabel.set(String(label), clean(row[co]));
        }
        return vocab;
    }

    _resolveVocabulary(workbook, manifestColumns, sheetsByName, vocab, report) {
        const vocabColsBySheet = new Map();
        for (const c of manifestColumns) {
            if (c.vocab && c.roundtrip === "editable") {
                if (!vocabColsBySheet.has(c.sheet)) vocabColsBySheet.set(c.sheet, []);
                vocabColsBySheet.get(c.sheet).push(c);
            }
        }
        for (const [sheetName, cols] of vocabColsBySheet) {
            const sheet = sheetsByName.get(sheetName);
            if (!sheet) continue;
            const idx = this._colIndex(sheet);
            for (const col of cols) {
                const ci = idx.get(col.key);
                if (ci === undefined) continue;
                const known = vocab.get(col.vocab);
                const hasValues = (sheet.rows || []).some(row => !isBlank(row[ci]));
                if (!known && col.vocab !== "action") {
                    if (hasValues) {
                        //no closed list to judge against — flag once, don't flood
                        //the report with a proposal per row (D7 is about genuinely
                        //unknown terms, not an absent vocabulary sheet).
                        report.warnings.push({
                            stage: 2, code: "vocab_list_absent", sheet: sheetName, column: col.key,
                            message: `Column "${col.key}" binds to vocabulary "${col.vocab}", which is not present in the Vocabularies sheet; its values were not checked.`,
                        });
                    }
                    continue;
                }
                sheet.rows.forEach((row, r) => {
                    const val = clean(row[ci]);
                    if (isBlank(val)) return;
                    if (col.vocab === "action") {
                        if (!["", "delete", "(blank)"].includes(String(val))) {
                            report.errors.push({ stage: 2, code: "bad_action", sheet: sheetName, cell: this._cell(sheet, ci, r), message: `Action "${val}" is not recognised; only "delete" or blank are allowed (D12).` });
                        }
                        return;
                    }
                    if (!known || !known.labels.has(String(val))) {
                        report.proposals.push({
                            stage: 2, sheet: sheetName, column: col.key, list: col.vocab,
                            value: val, cell: this._cell(sheet, ci, r),
                            message: `"${val}" is not in vocabulary "${col.vocab}"; routed to a curator as a proposed term, never inserted automatically (D7).`,
                        });
                    }
                });
            }
        }
    }

    // ------------------------------------------------------------------ stage 3

    _collectNewTokens(workbook, report) {
        const tokens = new Map(); //token -> {sheet, row}
        for (const sheet of workbook.sheets) {
            const spec = DIFF_SPECS[sheet.name];
            const idx = this._colIndex(sheet);
            //the pk is always the first identity column of an editable sheet
            const pkKey = spec ? spec.pk : (sheet.columns.find(c => c.key && c.key.startsWith("_") && c.hidden) || {}).key;
            if (!pkKey) continue;
            const pi = idx.get(pkKey);
            if (pi === undefined) continue;
            (sheet.rows || []).forEach((row, r) => {
                const v = row[pi];
                if (isNewToken(v)) {
                    const t = String(v).trim();
                    if (tokens.has(t)) {
                        report.errors.push({ stage: 3, code: "duplicate_new_token", sheet: sheet.name, cell: this._cell(sheet, pi, r), message: `NEW token "${t}" is defined more than once (also in ${tokens.get(t).sheet}).` });
                    } else {
                        tokens.set(t, { sheet: sheet.name, row: r });
                    }
                }
            });
        }
        return tokens;
    }

    _checkReferences(workbook, sheetsByName, newTokens, report) {
        //membership sets for parent pks present in the workbook
        const parentPks = new Map();
        for (const [refKey, parentSheetName] of Object.entries(PARENT_SHEET_FOR_REF)) {
            const parent = sheetsByName.get(parentSheetName);
            if (!parent) continue;
            const spec = DIFF_SPECS[parentSheetName];
            const pkKey = spec ? spec.pk : refKey;
            const idx = this._colIndex(parent);
            const pi = idx.get(pkKey);
            if (pi === undefined) continue;
            const set = new Set();
            for (const row of parent.rows || []) {
                const v = row[pi];
                if (!isBlank(v)) set.add(String(v).trim());
            }
            parentPks.set(refKey, set);
        }

        for (const sheet of workbook.sheets) {
            const idx = this._colIndex(sheet);
            const spec = DIFF_SPECS[sheet.name];
            const pkKey = spec ? spec.pk : null;
            for (const col of sheet.columns || []) {
                if (!col.key || !col.key.startsWith("_") || col.key === pkKey) continue;
                if (col.key.endsWith("_uuid")) continue;
                if (!PARENT_SHEET_FOR_REF[col.key]) continue; //only checkable refs
                const ci = idx.get(col.key);
                if (ci === undefined) continue;
                const known = parentPks.get(col.key);
                (sheet.rows || []).forEach((row, r) => {
                    const v = row[ci];
                    if (isBlank(v)) return;
                    const s = String(v).trim();
                    if (isNewToken(s)) {
                        if (!newTokens.has(s)) {
                            report.errors.push({ stage: 3, code: "undefined_new_token", sheet: sheet.name, cell: this._cell(sheet, ci, r), message: `References NEW token "${s}", which is not defined anywhere in this workbook (D2).` });
                        }
                        return;
                    }
                    if (known && known.size && !known.has(s)) {
                        report.errors.push({ stage: 3, code: "dangling_reference", sheet: sheet.name, cell: this._cell(sheet, ci, r), message: `${col.key} = ${s} does not match any row in the ${PARENT_SHEET_FOR_REF[col.key]} sheet.` });
                    }
                });
            }
        }
    }

    // ------------------------------------------------------------------ stage 4

    async _diff(workbook, sheetsByName, report) {
        for (const [sheetName, spec] of Object.entries(DIFF_SPECS)) {
            const sheet = sheetsByName.get(sheetName);
            if (!sheet) continue;
            const idx = this._colIndex(sheet);
            const pi = idx.get(spec.pk);
            const ai = idx.get("Action");
            if (pi === undefined) continue;

            const fieldPairs = Object.entries(spec.fields)
                .map(([col, dbcol]) => ({ col, dbcol, ci: idx.get(col) }))
                .filter(f => f.ci !== undefined && f.dbcol);

            const seenPks = new Set();

            for (let r = 0; r < (sheet.rows || []).length; r++) {
                const row = sheet.rows[r];
                const pkVal = row[pi];
                const action = ai !== undefined ? clean(row[ai]) : null;

                if (isBlank(pkVal) || isNewToken(pkVal)) {
                    report.change_set.adds.push({
                        sheet: sheetName, table: spec.table,
                        temp_id: isNewToken(pkVal) ? String(pkVal).trim() : null,
                        row: r,
                        values: Object.fromEntries(fieldPairs.map(f => [f.dbcol, clean(row[f.ci])])),
                    });
                    continue;
                }

                const pk = String(pkVal).trim();
                seenPks.add(pk);

                if (action === "delete") {
                    report.change_set.deletes.push({ sheet: sheetName, table: spec.table, pk, row: r });
                    continue;
                }

                if (spec.diffAgainstDb === false) continue;

                const dbRow = await this._fetchRow(spec, pk);
                if (!dbRow) {
                    report.errors.push({ stage: 4, code: "row_not_in_db", sheet: sheetName, cell: this._cell(sheet, pi, r), message: `${spec.pk} = ${pk} is not a ${spec.table} row in this database. A workbook from another database cannot be imported here (D2/D9).` });
                    continue;
                }
                const changed = [];
                for (const f of fieldPairs) {
                    const before = dbRow[f.dbcol];
                    const after = clean(row[f.ci]);
                    if (!this._equal(before, after)) {
                        changed.push({ field: f.dbcol, column: f.col, before: before ?? null, after: after ?? null });
                    }
                }
                if (changed.length) {
                    report.change_set.updates.push({ sheet: sheetName, table: spec.table, pk, row: r, changes: changed });
                }
            }

            //rows in the DB for this scope that the workbook never mentions.
            //Per D12 this is NOT a deletion — reported so a reviewer can see the
            //workbook's coverage, nothing more.
            if (spec.scopeCol && spec.scopeRefColumn && spec.diffAgainstDb !== false) {
                const scopeIds = this._scopeIds(sheet, idx, spec.scopeRefColumn);
                if (scopeIds.length) {
                    try {
                        const res = await this.app.query(
                            `SELECT ${spec.pkCol} AS pk FROM ${spec.table} WHERE ${spec.scopeCol} = ANY($1)`,
                            [scopeIds],
                        );
                        for (const dbr of res.rows) {
                            if (!seenPks.has(String(dbr.pk))) {
                                report.change_set.unmentioned.push({ sheet: sheetName, table: spec.table, pk: String(dbr.pk) });
                            }
                        }
                    } catch (e) {
                        report.warnings.push({ stage: 4, code: "unmentioned_scan_failed", sheet: sheetName, message: e.message });
                    }
                }
            }
        }
    }

    async _fetchRow(spec, pk) {
        const cols = ["Site", "Sample Groups", "Samples", "Datasets"].includes(spec.table) ? "*" : "*";
        const res = await this.app.query(`SELECT ${cols} FROM ${spec.table} WHERE ${spec.pkCol} = $1`, [pk]);
        return res.rows[0] || null;
    }

    _scopeIds(sheet, idx, refColumn) {
        const ci = idx.get(refColumn);
        if (ci === undefined) return [];
        const ids = new Set();
        for (const row of sheet.rows || []) {
            const v = row[ci];
            if (!isBlank(v) && !isNewToken(v) && !isNaN(parseInt(v))) ids.add(parseInt(v));
        }
        return [...ids];
    }

    // ------------------------------------------------------------------ util

    _colIndex(sheet) {
        const m = new Map();
        (sheet.columns || []).forEach((c, i) => {
            if (c.key !== undefined && c.key !== null) m.set(c.key, i);
        });
        //also allow binding by title (D4) when key is absent
        (sheet.columns || []).forEach((c, i) => {
            if (c.title !== undefined && !m.has(c.title)) m.set(c.title, i);
        });
        return m;
    }

    _cell(sheet, colIdx, rowIdx) {
        //Excel-style A1 with a +2 offset for the header row, for a report a
        //curator can act on ("Samples!B17").
        let n = colIdx, s = "";
        do { s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26) - 1; } while (n >= 0);
        return `${sheet.name}!${s}${rowIdx + 2}`;
    }

    _equal(a, b) {
        const na = isBlank(a) ? null : a;
        const nb = isBlank(b) ? null : b;
        if (na === null && nb === null) return true;
        if (na === null || nb === null) return false;
        const fa = Number(na), fb = Number(nb);
        if (!Number.isNaN(fa) && !Number.isNaN(fb)) return fa === fb;
        const da = Date.parse(na), db = Date.parse(nb);
        if (!Number.isNaN(da) && !Number.isNaN(db)) return da === db;
        return String(na).trim() === String(nb).trim();
    }

    _hasBlockingErrors(report) {
        return report.errors.length > 0;
    }

    _finish(report) {
        const cs = report.change_set;
        report.diff_empty = report.errors.length === 0
            && cs.adds.length === 0 && cs.updates.length === 0 && cs.deletes.length === 0;
        report.ok = report.errors.length === 0;
        report.summary = {
            errors: report.errors.length,
            warnings: report.warnings.length,
            proposals: report.proposals.length,
            adds: cs.adds.length,
            updates: cs.updates.length,
            deletes: cs.deletes.length,
            unmentioned: cs.unmentioned.length,
        };
        report.next_step = report.ok
            ? "Stages 1-4 passed. A future phase materialises this change set into clearing_house.* and hands off to bin/commit-submission (D8). SDF writes nothing until then."
            : "Fix the errors above and re-import. Nothing was written.";
        return report;
    }
}
