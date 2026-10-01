/**
 * Stage 4, the three-way comparison (spec §10): workbook W, baseline B (the
 * row as exported, by hash) and live L, rule by rule, with synthetic inputs.
 * Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import SdfValidator from "../../src/Lib/SeadDataFormat/SdfValidator.class.js";
import { rowHash } from "../../src/Lib/SeadDataFormat/SdfCommon.js";

const KEYS = ["sample_id", "sample_name", "depth"];
const TABLE = { name: "tbl_samples", sheet: "samples", pk: "sample_id", fks: [] };

/**
 * Runs _diff on one row. `exported` is the row as exported (it becomes the
 * baseline hash), `workbook` the curator's copy, `live` the database now (null
 * when deleted). Returns the change set and the report.
 */
async function diff({ exported, workbook, live, proposed = {}, baseKeys = KEYS, dataKeys = KEYS, columnsGone = [], hashKeys = baseKeys }) {
    const report = {
        errors: [], warnings: [],
        error(stage, code, message) { this.errors.push({ code, message }); },
        warning(stage, code, message) { this.warnings.push({ code, message }); },
    };
    const record = {
        table: TABLE.name, sheet: TABLE.sheet, row: 2, action: null,
        pk: { kind: "id", value: workbook.sample_id },
        values: new Map(dataKeys.map(k => [k, workbook[k] ?? null])),
        cells: new Map(dataKeys.map((k, i) => [k, `${String.fromCharCode(66 + i)}2`])),
        proposed: new Map(Object.entries(proposed).map(([k, v]) => [k, { value: v, address: "Z2" }])),
        binding: { role: "owned", baseKeys, columnsGone, data: new Map(dataKeys.map((k, i) => [k, i + 2])) },
    };
    const ctx = {
        report,
        schema: { table: () => TABLE, tables: new Map([[TABLE.name, TABLE]]) },
        records: [record],
        live: new Map([[TABLE.name, new Map(live ? [[live.sample_id, live]] : [])]]),
        baseline: new Map([[`${TABLE.name}:${exported.sample_id}`, { hash: rowHash(hashKeys.map(k => exported[k] ?? null)), shared: false }]]),
        schemaProposals: Object.keys(proposed).map(column => ({ kind: "schema", type: "column", table: TABLE.name, sheet: TABLE.sheet, column })),
        bundleSiteIds: [1],
        client: { query: async () => ({ rows: [] }) },
    };
    await new SdfValidator({})._diff(ctx);
    return { cs: ctx.changeSet, report };
}

const row = (name, depth = 1.5) => ({ sample_id: 7, sample_name: name, depth });

test("rule 1: workbook equals live - nothing to do", async () => {
    const { cs } = await diff({ exported: row("a"), workbook: row("b"), live: row("b") });
    assert.equal(cs.updates.length + cs.conflicts.length, 0);
});

test("rule 2: only the database changed - the newer value is kept", async () => {
    const { cs } = await diff({ exported: row("a"), workbook: row("a"), live: row("a, corrected") });
    assert.equal(cs.updates.length + cs.conflicts.length, 0);
});

test("rule 3: only the workbook changed - an update of the changed column", async () => {
    const { cs } = await diff({ exported: row("a"), workbook: row("b"), live: row("a") });
    assert.equal(cs.updates.length, 1);
    assert.deepEqual(cs.updates[0].fields.map(f => [f.column, f.before, f.after]), [["sample_name", "a", "b"]]);
});

test("rule 4: both changed - a conflict, nothing applied", async () => {
    const { cs } = await diff({ exported: row("a"), workbook: row("b"), live: row("c") });
    assert.equal(cs.updates.length, 0);
    assert.equal(cs.conflicts.length, 1);
    assert.equal(cs.conflicts[0].reason, "changed_on_both_sides");
});

test("rule 2 with a proposed column: no stale write, the row waits on the proposal (C1)", async () => {
    const { cs } = await diff({ exported: row("a"), workbook: row("a"), live: row("a, corrected"), proposed: { texture: "clay" } });
    assert.equal(cs.updates.length, 0);
    assert.equal(cs.blocked.length, 1);
    assert.equal(cs.blocked[0].reason, "proposed_columns_only");
});

test("rule 3 with a proposed column: the edit applies, the proposed value waits", async () => {
    const { cs } = await diff({ exported: row("a"), workbook: row("b"), live: row("a"), proposed: { texture: "clay" } });
    assert.equal(cs.updates.length, 1);
    assert.deepEqual({ ...cs.updates[0].deferred }, { texture: "clay" });
});

test("rule 6: an untouched row deleted upstream needs no decision (M1)", async () => {
    const { cs, report } = await diff({ exported: row("a"), workbook: row("a"), live: null });
    assert.equal(cs.conflicts.length, 0);
    assert.deepEqual(report.warnings.map(w => w.code), ["deleted_since_export"]);
});

test("rule 6: an edited row deleted upstream is a conflict", async () => {
    const { cs } = await diff({ exported: row("a"), workbook: row("b"), live: null });
    assert.equal(cs.conflicts.length, 1);
    assert.equal(cs.conflicts[0].reason, "deleted_since_export");
});

test("a column added since export still compares: only it is updated", async () => {
    const keys = [...KEYS, "colour"];
    const { cs } = await diff({
        exported: row("a"), workbook: { ...row("a"), colour: "red" }, live: { ...row("a, corrected"), colour: null },
        baseKeys: KEYS, dataKeys: keys,
    });
    assert.equal(cs.updates.length, 1);
    assert.deepEqual(cs.updates[0].fields.map(f => f.column), ["colour"]);
});

test("line endings alone are not an edit", async () => {
    const { cs } = await diff({ exported: row("a\r\nb"), workbook: row("a\nb"), live: row("a\r\nb") });
    assert.equal(cs.updates.length + cs.conflicts.length, 0);
});

test("after a column is removed, a differing row is a conflict that says why; an equal row is fine", async () => {
    //exported with a "colour" column that the database has since dropped
    const gone = { exported: { ...row("a"), colour: "red" }, hashKeys: [...KEYS, "colour"], baseKeys: KEYS, columnsGone: ["colour"] };
    let { cs } = await diff({ ...gone, workbook: row("b"), live: row("a") });
    assert.equal(cs.updates.length, 0);
    assert.equal(cs.conflicts[0]?.reason, "columns_removed_since_export");
    ({ cs } = await diff({ ...gone, workbook: row("a"), live: row("a") }));
    assert.equal(cs.updates.length + cs.conflicts.length, 0);
});

test("a column sorted on its own is flagged (M17)", async () => {
    const report = { errors: [], warnings: [], error() {}, warning(stage, code) { this.warnings.push(code); } };
    const update = (row, before, after) => ({ table: "t", sheet: "samples", row, id: row, fields: [{ column: "sample_name", before, after }] });
    const ctx = { report, records: [], changeSet: { updates: [update(2, "a", "b"), update(3, "b", "c"), update(4, "c", "a")] } };
    new SdfValidator({})._warnSuspiciousEdits(ctx);
    assert.deepEqual(report.warnings, ["column_shuffled"]);
});

test("invisible edits are flagged (L11)", async () => {
    const report = { errors: [], warnings: [], error() {}, warning(stage, code) { this.warnings.push(code); } };
    const update = (row, before, after) => ({ table: "t", sheet: "samples", row, id: row, fields: [{ column: "sample_name", before, after }] });
    const ctx = { report, records: [], changeSet: { updates: [update(2, "Caf\u00e9", "Cafe\u0301"), update(3, "a  b", "a b "), update(4, "x", "y")] } };
    new SdfValidator({})._warnSuspiciousEdits(ctx);
    assert.deepEqual(report.warnings, ["invisible_edit", "invisible_edit"]);
});
