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
            "3. Do not add or delete rows on site data. For now, only changes to rows that already exist can be imported. Removing a row from the sheet does nothing at all.",
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
        ["Pick a value from a list", "Choose it in the …:label column's dropdown and clear the ID column next to it. The ID is looked up for you. If the ID column is filled in, the ID is used."],
        ["Use a taxon, reference, location, contact, project or relative age not in this file", "These lists only contain entries this site already uses. Look the entry up at browser.sead.se and type its ID into the ID column."],
        ["Add or delete rows", `Not supported yet. Send the data, or the rows to remove, to ${contact}, quoting the Export ID above.`],
        ["Suggest a new term (e.g. a sample type or taxon that does not exist)", "Add a row to that list's sheet with an ID such as NEW-1, and use that name in the ID column where you need the term. New terms are reviewed separately. Changes that use them are left out until the term is accepted (section 7)."],
        ["Suggest a new column or table", "Add the column, or a new sheet. A new sheet needs an ID column ending in _id, and a column linking each row to this site's data, such as physical_sample_id or analysis_entity_id. Structural changes need a data manager's design and review. The rest of your changes go ahead meanwhile, and the values you entered are sent along with the suggestion."],
    ];
    for (const [label, text] of howTo) {
        blocks.push({ type: "row", strong: true, label, text });
    }

    //4 · Colours
    blocks.push({ type: "section", text: "4 · Colours" });
    blocks.push({ type: "row", label: "Light blue columns", legend: "key", text: "IDs. Needed to match rows to the database. Edit only to point a row at a different entry, or to use a NEW- name you suggested." });
    blocks.push({ type: "row", label: "Grey columns", legend: "readonly", text: "For reading only: labels and last-updated dates." });
    blocks.push({ type: "row", label: "Amber column", legend: "action", text: "_action. On shared lists, type delete to suggest removing an entry. On site data, leave it empty: deleting rows is not supported yet." });
    blocks.push({ type: "row", label: "Green rows", legend: "newRow", text: "New entries suggested for a shared list (ID starts with NEW-)." });
    blocks.push({ type: "row", label: "Red rows", legend: "deleteRow", text: "Entries suggested for removal from a shared list." });
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
        text: `This site has no rows in these tables. Adding rows is not supported yet; to add data to one of them, contact ${contact}.`,
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
        text: config.validateUrl
            ? `When the check is clean, generate the change request from the same page and send it to the SEAD data managers at ${contact}. They review it and include it in a release.`
            : "A SEAD data manager checks the file, turns your changes into a change request, and includes it in a release. If the check finds problems, you will be asked to correct them.",
    });
    blocks.push({
        type: "row", strong: true, label: "3 · Suggestions",
        text: "If you suggested new terms, columns or tables, the changes that depend on them are left out. Once your suggestions are accepted and released, export a fresh file from SEAD and make those changes there. This file cannot be submitted twice.",
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
