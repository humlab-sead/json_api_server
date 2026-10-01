/**
 * The README sheet: the user guide every SDF workbook opens on (spec §11).
 *
 * The wording is the reference text in Appendix D of the specification and
 * should change only together with it. Values in {braces} there are filled in
 * here. The colour legend in section 4 describes SdfRenderer's COLOURS and must
 * be kept in step with it.
 *
 * Deployment-specific values come from the environment:
 *   SDF_LICENCE       licence statement for exported data
 *   SDF_CITATION      how to cite SEAD
 *   SDF_CONTACT       where curators send change requests and questions
 *   SDF_VALIDATE_URL  where an edited workbook is uploaded for checking
 */

const DEFAULT_CONTACT = "support@humlab.umu.se";

export function guideConfig(env = process.env) {
    return {
        licence: env.SDF_LICENCE || null,
        citation: env.SDF_CITATION || null,
        contact: env.SDF_CONTACT || DEFAULT_CONTACT,
        validateUrl: env.SDF_VALIDATE_URL || null,
    };
}

/**
 * Returns the guide as a list of blocks for the renderer:
 *   { type: "title", text }
 *   { type: "section", text }
 *   { type: "row", label, text, strong }
 *   { type: "paragraph", text }
 *   { type: "table", header: [...], rows: [[...]] }
 */
export function buildGuide(bundle, config) {
    const meta = new Map(bundle.meta);
    const siteIds = (meta.get("site_ids") || "").split(",");
    const siteNames = (meta.get("site_names") || "").split("\n");
    const sites = siteIds.map((id, i) => `${id} — ${siteNames[i] ?? ""}`).join("\n");
    const contact = config.contact;
    const blocks = [];

    blocks.push({ type: "title", text: `SEAD site export — ${siteNames.join(", ")}` });

    //1 · About this file
    blocks.push({ type: "section", text: "1 · About this file" });
    blocks.push({ type: "row", label: "Sites", text: sites });
    blocks.push({ type: "row", label: "Exported", text: `${formatUtc(meta.get("exported_at"))} by ${meta.get("exported_by") || "anonymous"}` });
    blocks.push({
        type: "row", label: "Source database",
        text: `${meta.get("database_name")}` +
            (meta.get("database_release") ? ` — SEAD release ${meta.get("database_release")}` : "") +
            ". The exact database state is recorded in the hidden sheet _sdf_meta.",
    });
    blocks.push({ type: "row", label: "Export ID", text: `${meta.get("export_id")} — quote this if you contact us about this file.` });
    blocks.push({ type: "row", label: "Format", text: `SEAD Data Format ${meta.get("sdf_version")}, written by ${meta.get("exporter")}` });
    blocks.push({
        type: "row", label: "Licence and citation",
        text: [
            config.licence ? `${config.licence}.` : `Contact ${contact} for the terms under which this data may be reused.`,
            "Cite the original publications listed on the biblio sheet" +
                (config.citation ? `, and SEAD as ${config.citation}.` : ", and SEAD."),
        ].join(" "),
    });

    //2 · Read this first
    blocks.push({ type: "section", text: "2 · Read this first" });
    blocks.push({
        type: "row", strong: true, label: "What this file is",
        text: "A complete copy of everything SEAD holds about this site: every table, every row, every column. Each sheet is one database table, and row 1 of each sheet holds the database's column names.",
    });
    blocks.push({
        type: "row", strong: true, label: "You can edit it and send it back",
        text: "Changes you make here can be checked and submitted to SEAD. Nothing you do in this file changes SEAD directly. Every change is reviewed by a SEAD data manager and appears in SEAD after the next release.",
    });
    blocks.push({
        type: "row", strong: true, label: "It belongs to one database",
        text: "The IDs in this file are only valid in the database it came from (above). It stays valid when SEAD is updated. If someone else changes the same data meanwhile, that is detected and shown to you, never silently overwritten.",
    });
    blocks.push({
        type: "row", strong: true, label: "The eight rules",
        text: [
            "1. Do not rename sheets or change the column names in row 1. They are how your changes are matched to the database.",
            "2. Do not delete columns. Hide the ones you do not need.",
            "3. To delete a row, type delete in its _action column. Removing a row from the sheet does nothing at all.",
            "4. Grey columns, those ending in :label and date_updated, are for reading. Changes to them are ignored.",
            "5. Enter numbers as numbers. A number Excel has stored as text, such as 1,5 pasted from elsewhere, is rejected rather than guessed at.",
            "6. Do not use formulas in data columns. Paste values instead.",
            "7. Save as .xlsx. CSV files cannot be imported.",
            "8. Keep this file until your changes appear in SEAD. You may need it again.",
        ].join("\n"),
    });

    //3 · How to…
    blocks.push({ type: "section", text: "3 · How to…" });
    const howTo = [
        ["Change a value", "Edit the cell."],
        ["Add a row", "Add it at the bottom of the table on that sheet. Leave the first ID column (e.g. physical_sample_id) empty, or give it a name such as NEW-1 if other rows need to refer to it."],
        ["Link new rows together", "Names starting with NEW- stand in for IDs that do not exist yet. Example: add a sample group with sample_group_id = NEW-1 on sample_groups, then put NEW-1 in the sample_group_id column of each new sample on physical_samples. Use NEW- followed by letters, digits, - or _. Each name must be used for only one new row on a sheet."],
        ["Pick a value from a list", "Choose it in the …:label column's dropdown and leave the ID column next to it empty. The ID is looked up for you. If you fill in the ID, the ID is used and the label ignored."],
        ["Use a taxon, reference, location, contact, project or relative age not in this file", "These lists only contain entries this site already uses. Look the entry up at browser.sead.se and type its ID into the ID column."],
        ["Delete a row", "Type delete in _action. Rows that refer to it, on any sheet, must be deleted too. For example, deleting a sample means also deleting its descriptions, dimensions and analysis entities. The check lists anything you missed. Nothing is ever deleted automatically."],
        ["Add data to a table that has no sheet here", "Add a sheet named exactly as listed under \"Tables not in this file\" below, and copy its column names into row 1. You only need the columns you fill, plus the ID column."],
        ["Suggest a new term (e.g. a sample type or taxon that does not exist)", "Add a row to that list's sheet with an ID such as NEW-1, and use that name where you need the term. New terms are reviewed separately. Rows that use them are held back until the term is accepted."],
        ["Suggest a new column or table", "Add the column, or a new sheet. A new sheet needs an ID column ending in _id, and a column linking each row to this site's data, such as physical_sample_id or analysis_entity_id. Structural changes need a data manager's design and review. The rest of your changes go ahead meanwhile, and you get a follow-up file for the held-back part (section 7)."],
    ];
    for (const [label, text] of howTo) {
        blocks.push({ type: "row", strong: true, label, text });
    }

    //4 · Colours
    blocks.push({ type: "section", text: "4 · Colours" });
    blocks.push({ type: "row", label: "Light blue columns", legend: "key", text: "IDs. Needed to match rows to the database. Edit only to write a NEW- name, or an ID when linking rows." });
    blocks.push({ type: "row", label: "Grey columns", legend: "readonly", text: "For reading only: labels and last-updated dates." });
    blocks.push({ type: "row", label: "Amber column", legend: "action", text: "_action. Leave it empty, or type delete." });
    blocks.push({ type: "row", label: "Green rows", legend: "newRow", text: "New rows (ID starts with NEW-)." });
    blocks.push({ type: "row", label: "Red rows", legend: "deleteRow", text: "Rows marked for deletion." });
    blocks.push({ type: "row", label: "Grey sheet tabs", legend: "referenceTab", text: "Shared lists (taxa, methods, types, …). Changes here are suggestions for review, not edits." });

    //5 · Sheets in this file
    blocks.push({ type: "section", text: "5 · Sheets in this file" });
    blocks.push({
        type: "table",
        header: ["Sheet", "Contains", "Rows", "Kind"],
        rows: bundle.sheets.map(s => [s.name, s.description || "", s.rows.length, sheetKind(s)]),
    });

    //6 · Tables not in this file
    blocks.push({ type: "section", text: "6 · Tables not in this file" });
    blocks.push({
        type: "paragraph",
        text: "This site has no rows in these tables. To add some, add a sheet with the name below and put the column names in row 1.",
    });
    blocks.push({
        type: "table",
        header: ["Sheet name to use", "Contains", "Column names"],
        rows: bundle.missingOwnedTables.map(t => [t.sheet, t.description || "", t.columns.join(", ")]),
    });

    //7 · When you are done
    blocks.push({ type: "section", text: "7 · When you are done" });
    blocks.push({
        type: "row", strong: true, label: "1 · Check",
        text: config.validateUrl
            ? `Upload the file at ${config.validateUrl}. Nothing is changed. You get a report listing any errors with the exact cell (e.g. physical_samples!E17), any conflicts with changes made in SEAD since your export, and every change your file would make. Fix and upload again until it is clean.`
            : `Checking an edited file online is not available yet. Until it is, send the file to ${contact}, quoting the Export ID above.`,
    });
    blocks.push({
        type: "row", strong: true, label: "2 · Submit",
        text: `When the check is clean, generate the change request from the same page and send it to the SEAD data managers at ${contact}. They review it and include it in a release.`,
    });
    blocks.push({
        type: "row", strong: true, label: "3 · Follow-up",
        text: "If you suggested new terms, columns or tables, the part that depends on them is held back. You will receive a followup.xlsx. Once your suggestions are accepted and released, upload that file, not this one, to submit the rest. This file cannot be submitted twice.",
    });
    blocks.push({ type: "row", strong: true, label: "Questions", text: `${contact}, quoting the Export ID above.` });

    return blocks;
}

function sheetKind(sheet) {
    if (sheet.role === "owned") return "site data";
    if (sheet.role === "reference-deprecated") return "read-only legacy";
    return sheet.referenceMode === "full" ? "shared list, full" : "shared list, entries used here";
}

function formatUtc(iso) {
    return iso ? iso.replace("T", " ").replace("Z", " UTC") : "";
}
