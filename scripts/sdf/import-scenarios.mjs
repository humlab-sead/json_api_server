/**
 * SDF import scenarios (plans/sead-data-format-design.html §2, T3, validator part).
 *
 * Exports real sites from a running server, edits the workbooks the way a
 * curator would, uploads them to /sdf/validate and checks that the report says
 * exactly what it should. Nothing is written to any database.
 *
 * Usage, from the json_api_server directory (inside the container):
 *   node scripts/sdf/import-scenarios.mjs [--base http://localhost:8484] [--site 1] [--shared-site 321]
 * Exits non-zero if any scenario fails.
 */
import ExcelJS from "exceljs";
import { canonicalCsv, sha256Hex, rowHash } from "../../src/Lib/SeadDataFormat/SdfCommon.js";
import { plainRows, readCell } from "../../src/Lib/SeadDataFormat/SdfWorkbookReader.js";

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, a, i, all) =>
    (a.startsWith("--") ? [...acc, [a.slice(2), all[i + 1]]] : acc), []));
const base = args.base || "http://localhost:8484";
const SITE = Number(args.site || 1);
const SHARED_SITE = Number(args["shared-site"] || 321);

let failures = 0;
const check = (name, condition, detail) => {
    console.log(`${condition ? "  ok  " : "  FAIL"} ${name}${!condition && detail ? `\n         ${JSON.stringify(detail).slice(0, 600)}` : ""}`);
    if (!condition) failures++;
};

async function exportWorkbook(siteId) {
    const res = await fetch(`${base}/sdf/export/${siteId}`);
    if (res.status !== 200) throw new Error(`export ${siteId}: ${res.status}`);
    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(Buffer.from(await res.arrayBuffer()));
    return wb;
}

async function validate(wb) {
    const buffer = Buffer.from(await wb.xlsx.writeBuffer());
    const res = await fetch(`${base}/sdf/validate`, {
        method: "POST",
        headers: { "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" },
        body: buffer,
    });
    return await res.json();
}

/** Column index (1-based) of a header on a sheet. */
function col(ws, key) {
    const row = ws.getRow(1);
    for (let c = 1; c <= ws.columnCount; c++) if (row.getCell(c).value === key) return c;
    throw new Error(`no column ${key} on ${ws.name}`);
}

function firstRowWhere(ws, predicate) {
    for (let r = 2; r <= ws.rowCount; r++) if (predicate(ws.getRow(r), r)) return r;
    return null;
}

/** Test-only: rewrite a baseline hash and re-sign the machine sheets. */
function tamperBaseline(wb, table, id, hash) {
    const ws = wb.getWorksheet("_sdf_baseline");
    for (let r = 2; r <= ws.rowCount; r++) {
        const row = ws.getRow(r);
        if (row.getCell(1).value === table && row.getCell(2).value === id) row.getCell(3).value = hash;
    }
    const checksum = sha256Hex(canonicalCsv(plainRows(wb.getWorksheet("_sdf_columns"))) + canonicalCsv(plainRows(ws)));
    const meta = wb.getWorksheet("_sdf_meta");
    for (let r = 2; r <= meta.rowCount; r++) if (meta.getRow(r).getCell(1).value === "checksum") meta.getRow(r).getCell(2).value = checksum;
}

/**
 * Test-only: simulate "the database changed this row after export" without
 * touching the database. The workbook row is given an older value for one
 * column, and the baseline is re-hashed to match, so workbook = baseline while
 * the live row differs (rule 2's situation).
 */
function simulateDatabaseChange(wb, sheetName, rowNumber, column, olderValue) {
    const ws = wb.getWorksheet(sheetName);
    const columns = plainRows(wb.getWorksheet("_sdf_columns"));
    const h = columns[0];
    const dataKeys = columns.slice(1).filter(r => r[h.indexOf("sheet")] === sheetName && r[h.indexOf("kind")] === "data").map(r => r[h.indexOf("key")]);
    const table = columns.find(r => r[h.indexOf("sheet")] === sheetName)[h.indexOf("table")];
    ws.getRow(rowNumber).getCell(col(ws, column)).value = olderValue;
    const values = dataKeys.map(k => readCell(ws.getRow(rowNumber).getCell(col(ws, k))).value);
    const pk = ws.getRow(rowNumber).getCell(col(ws, dataKeys[0])).value;
    tamperBaseline(wb, table, pk, rowHash(values));
    return pk;
}

const codes = report => (report.errors || []).map(e => e.code);

//-------------------------------------------------------------------------

console.log(`Scenario A: a valid set of edits on site ${SITE}`);
{
    const wb = await exportWorkbook(SITE);
    const ps = wb.getWorksheet("physical_samples");
    const types = wb.getWorksheet("sample_types");

    //update a sample name, keeping a leading zero
    ps.getRow(2).getCell(col(ps, "sample_name")).value = "0123 edited";

    //change a sample's type by label: clear the id, pick another type's label
    const typeNow = ps.getRow(5).getCell(col(ps, "sample_type_id")).value;
    const tOther = firstRowWhere(types, row => row.getCell(col(types, "sample_type_id")).value !== typeNow);
    const otherId = types.getRow(tOther).getCell(col(types, "sample_type_id")).value;
    ps.getRow(5).getCell(col(ps, "sample_type_id")).value = null;
    ps.getRow(5).getCell(col(ps, "sample_type_id:label")).value = types.getRow(tOther).getCell(col(types, "type_name")).value;

    //propose a new sample type, and point an existing sample at it (blocked)
    const tRow = types.rowCount + 1;
    types.getRow(tRow).getCell(col(types, "sample_type_id")).value = "NEW-typeX";
    types.getRow(tRow).getCell(col(types, "type_name")).value = "SDF test type";
    const blockedId = ps.getRow(4).getCell(col(ps, "physical_sample_id")).value;
    ps.getRow(4).getCell(col(ps, "sample_type_id")).value = "NEW-typeX";

    //propose a new column with a value on an existing sample (blocked on the column)
    const texture = ps.columnCount + 1;
    ps.getRow(1).getCell(texture).value = "texture";
    ps.getRow(3).getCell(texture).value = "clay";

    //propose a new table attached to a sample
    const nt = wb.addWorksheet("sample_textures");
    nt.addRow(["sample_texture_id", "physical_sample_id", "texture"]);
    nt.addRow(["NEW-1", ps.getRow(3).getCell(col(ps, "physical_sample_id")).value, "silty clay"]);

    const r = await validate(wb);
    const cs = r.change_set || {};
    check("validates", r.ok === true, r.errors);
    check("two updates: the sample name, and the type chosen by label", cs.updates?.length === 2 &&
        cs.updates.some(u => u.fields.some(f => f.column === "sample_name" && f.after === "0123 edited")) &&
        cs.updates.some(u => u.fields.some(f => f.column === "sample_type_id" && f.after === otherId)), cs.updates);
    check("no inserts or deletes", cs.inserts?.length === 0 && cs.deletes?.length === 0, { inserts: cs.inserts, deletes: cs.deletes });
    check("reference proposal for the new type", cs.proposals?.some(p => p.kind === "reference" && p.op === "insert" && p.table === "tbl_sample_types"), cs.proposals);
    check("schema proposal: column texture", cs.proposals?.some(p => p.kind === "schema" && p.type === "column" && p.column === "texture"), cs.proposals);
    check("schema proposal: table sample_textures", cs.proposals?.some(p => p.kind === "schema" && p.type === "table" && p.sheet === "sample_textures"), cs.proposals);
    check("the sample pointed at the proposed type is blocked", cs.blocked?.some(b => b.id === blockedId && b.reason === "depends_on_proposal"), cs.blocked);
    check("the sample with only a proposed value is blocked", cs.blocked?.some(b => b.reason === "proposed_columns_only"), cs.blocked);
    check("no conflicts", cs.conflicts?.length === 0, cs.conflicts);
}

console.log(`Scenario A2: adding and deleting rows is refused on site ${SITE}`);
{
    const wb = await exportWorkbook(SITE);
    const ps = wb.getWorksheet("physical_samples");
    const nr = ps.rowCount + 1;
    ps.getRow(nr).getCell(col(ps, "physical_sample_id")).value = "NEW-psA";
    ps.getRow(nr).getCell(col(ps, "sample_group_id")).value = ps.getRow(2).getCell(col(ps, "sample_group_id")).value;
    ps.getRow(nr).getCell(col(ps, "sample_name")).value = "0001";
    ps.getRow(nr).getCell(col(ps, "sample_type_id")).value = ps.getRow(2).getCell(col(ps, "sample_type_id")).value;
    const nr2 = nr + 1;
    ps.getRow(nr2).getCell(col(ps, "sample_group_id")).value = ps.getRow(2).getCell(col(ps, "sample_group_id")).value;
    ps.getRow(nr2).getCell(col(ps, "sample_name")).value = "blank id";
    const notes = wb.getWorksheet("sample_notes");
    notes.getRow(2).getCell(col(notes, "_action")).value = "delete";
    const r = await validate(wb);
    check("rejected at stage 2", r.ok === false && r.stage_reached === 2, { stage: r.stage_reached, codes: codes(r) });
    check("both new rows: insert_not_supported", codes(r).filter(c => c === "insert_not_supported").length === 2, r.errors);
    check("the delete mark: delete_not_supported", codes(r).includes("delete_not_supported"), codes(r));
    check("anchored to the ID and _action cells", r.errors.every(e => e.sheet && /^[A-Z]+\d+$/.test(e.cell || "")), r.errors);
}

console.log(`Scenario B: cell-level errors on site ${SITE}`);
{
    const wb = await exportWorkbook(SITE);
    const sites = wb.getWorksheet("sites");
    sites.getRow(2).getCell(col(sites, "latitude_dd")).value = "57,1";
    const ps = wb.getWorksheet("physical_samples");
    //what Excel does to 20-30 typed into a General cell: a date, with a date format
    ps.getRow(2).getCell(col(ps, "sample_name")).value = new Date(Date.UTC(2026, 2, 20));
    ps.getRow(2).getCell(col(ps, "sample_name")).numFmt = "d-mmm";
    ps.getRow(3).getCell(col(ps, "sample_name")).value = { formula: "A1&\"x\"", result: "x" };
    ps.getRow(4).getCell(col(ps, "_action")).value = "remove";
    ps.getRow(5).getCell(col(ps, "sample_name")).value = "NEW-oops";
    const r = await validate(wb);
    check("rejected at stage 2", r.ok === false && r.stage_reached === 2, { stage: r.stage_reached, codes: codes(r) });
    for (const code of ["decimal_comma", "date_in_text_column", "formula", "bad_action", "token_outside_key"]) {
        check(`reports ${code}`, codes(r).includes(code), codes(r));
    }
    check("errors are anchored to cells", r.errors.every(e => e.sheet && e.cell), r.errors);
}

console.log(`Scenario C: referential errors on site ${SITE}`);
{
    const wb = await exportWorkbook(SITE);
    const ps = wb.getWorksheet("physical_samples");
    //a token nobody defines, in an existing row
    ps.getRow(2).getCell(col(ps, "sample_group_id")).value = "NEW-nowhere";
    //a label that matches nothing, on an existing row whose id was cleared
    ps.getRow(3).getCell(col(ps, "sample_type_id")).value = null;
    ps.getRow(3).getCell(col(ps, "sample_type_id:label")).value = "No such sample type";
    //an id that does not exist
    ps.getRow(4).getCell(col(ps, "sample_type_id")).value = 99999999;
    //a row copied in from another site's workbook
    const other = await exportWorkbook(SHARED_SITE);
    const otherPs = other.getWorksheet("physical_samples");
    const nr = ps.rowCount + 1;
    ps.getRow(nr).getCell(col(ps, "physical_sample_id")).value = otherPs.getRow(2).getCell(col(otherPs, "physical_sample_id")).value;
    ps.getRow(nr).getCell(col(ps, "sample_group_id")).value = ps.getRow(2).getCell(col(ps, "sample_group_id")).value;
    ps.getRow(nr).getCell(col(ps, "sample_name")).value = "copied";
    ps.getRow(nr).getCell(col(ps, "sample_type_id")).value = ps.getRow(5).getCell(col(ps, "sample_type_id")).value;
    const r = await validate(wb);
    check("rejected at stage 3", r.ok === false && r.stage_reached === 3, { stage: r.stage_reached, codes: codes(r) });
    for (const code of ["unknown_token", "label_not_found", "fk_not_found", "row_not_in_bundle"]) check(`reports ${code}`, codes(r).includes(code), codes(r));
}

console.log(`Scenario D: three-way comparison on site ${SITE} (baseline altered to simulate a database change)`);
{
    const wb = await exportWorkbook(SITE);
    const ps = wb.getWorksheet("physical_samples");
    const idX = ps.getRow(2).getCell(col(ps, "physical_sample_id")).value;
    tamperBaseline(wb, "tbl_physical_samples", idX, "0000000000000000");
    ps.getRow(2).getCell(col(ps, "sample_name")).value = "edited on both sides";
    const idY = simulateDatabaseChange(wb, "physical_samples", 3, "sample_name", "older name");
    const r = await validate(wb);
    const cs = r.change_set || {};
    check("edited row changed on both sides is a conflict", cs.conflicts?.some(c => c.id === idX && c.reason === "changed_on_both_sides"), cs.conflicts);
    check("untouched row changed in the database is left alone (rule 2)", !cs.updates?.some(u => u.id === idY) && !cs.conflicts?.some(c => c.id === idY), { updates: cs.updates, conflicts: cs.conflicts });
}

console.log(`Scenario D2: a proposed column on a row the database changed since export, on site ${SITE}`);
{
    const wb = await exportWorkbook(SITE);
    const ps = wb.getWorksheet("physical_samples");
    const id = simulateDatabaseChange(wb, "physical_samples", 2, "sample_name", "older name");
    const texture = ps.columnCount + 1;
    ps.getRow(1).getCell(texture).value = "texture";
    ps.getRow(2).getCell(texture).value = "clay";
    const r = await validate(wb);
    const cs = r.change_set || {};
    check("validates", r.ok === true, r.errors);
    check("the stale exported values are not written back", !cs.updates?.some(u => u.id === id), cs.updates);
    check("the row is blocked on the column proposal", cs.blocked?.some(b => b.id === id && b.reason === "proposed_columns_only"), cs.blocked);
}

console.log(`Scenario E: shared rows on site ${SHARED_SITE}`);
{
    const wb = await exportWorkbook(SHARED_SITE);
    const ds = wb.getWorksheet("datasets");
    const sharedIds = plainRows(wb.getWorksheet("_sdf_baseline")).filter(r => r[0] === "tbl_datasets" && (r[3] === true || r[3] === "true")).map(r => r[1]);
    const rowA = firstRowWhere(ds, row => row.getCell(col(ds, "dataset_id")).value === sharedIds[0]);
    const rowB = firstRowWhere(ds, row => row.getCell(col(ds, "dataset_id")).value === sharedIds[1]);
    ds.getRow(rowA).getCell(col(ds, "dataset_name")).value = "Soil chemistry Hällekind (edited)";
    let r = await validate(wb);
    check("update of a shared dataset is flagged with the other site", r.change_set?.shared?.some(s => s.id === sharedIds[0] && s.other_site_ids?.length), r.change_set?.shared);
    ds.getRow(rowB).getCell(col(ds, "_action")).value = "delete";
    r = await validate(wb);
    check("delete of a shared dataset is refused", codes(r).includes("delete_not_supported"), codes(r));
}

console.log("Scenario F: structural refusals");
{
    const wb = await exportWorkbook(SITE);
    const ps = wb.getWorksheet("physical_samples");
    ps.getRow(1).getCell(col(ps, "sample_name")).value = "Sample name";
    let r = await validate(wb);
    check("renamed header: missing_column", codes(r).includes("missing_column"), codes(r));

    const wb2 = await exportWorkbook(SITE);
    wb2.getWorksheet("_sdf_baseline").getRow(2).getCell(3).value = "ffffffffffffffff";
    r = await validate(wb2);
    check("edited machine sheet: checksum_mismatch", codes(r).includes("checksum_mismatch"), codes(r));

    const res = await fetch(`${base}/sdf/validate`, { method: "POST", headers: { "Content-Type": "text/csv" }, body: "site_id,site_name\n1,x\n" });
    r = await res.json();
    check("CSV upload: csv_not_accepted", codes(r).includes("csv_not_accepted"), r);
}

console.log(failures ? `${failures} check(s) failed.` : "All checks passed.");
process.exit(failures ? 1 : 0);
