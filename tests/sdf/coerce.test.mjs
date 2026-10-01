/**
 * Stage 2 cell coercion (spec §8): what a cell may hold for each kind of
 * column, and what is refused rather than changed. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import SdfValidator from "../../src/Lib/SeadDataFormat/SdfValidator.class.js";
import { CELL } from "../../src/Lib/SeadDataFormat/SdfWorkbookReader.js";

const validator = new SdfValidator({});

function column(pgType, extra = {}) {
    const typname = {
        "integer": "int4", "smallint": "int2", "bigint": "int8", "numeric": "numeric", "numeric(18,10)": "numeric",
        "numeric(10,5)": "numeric", "character varying(5)": "varchar", "text": "text",
    }[pgType];
    const m = /numeric\((\d+),(\d+)\)/.exec(pgType);
    return {
        name: "c", pgType, typname,
        valueKind: { int4: "integer", int2: "integer", int8: "integer", numeric: "decimal" }[typname] || "text",
        maxLength: /\((\d+)\)$/.test(pgType) && typname === "varchar" ? Number(/\((\d+)\)$/.exec(pgType)[1]) : null,
        numericPrecision: m ? Number(m[1]) : null, numericScale: m ? Number(m[2]) : null,
        ...extra,
    };
}

/** Coerces one cell; returns { value, errors, warnings } as codes. */
function coerce(kind, value, col, isKey = false) {
    const report = { errors: [], warnings: [], error(stage, code) { this.errors.push(code); }, warning(stage, code) { this.warnings.push(code); } };
    const out = validator._coerceCell(report, { kind, value, address: "A1" }, col, isKey, {});
    return { value: out, errors: report.errors, warnings: report.warnings };
}

test("integers: whole numbers within the type's range", () => {
    assert.equal(coerce(CELL.NUMBER, 32767, column("smallint")).value, 32767);
    assert.deepEqual(coerce(CELL.NUMBER, 32768, column("smallint")).errors, ["integer_out_of_range"]);
    assert.deepEqual(coerce(CELL.NUMBER, 2147483648, column("integer")).errors, ["integer_out_of_range"]);
    assert.deepEqual(coerce(CELL.NUMBER, 1.5, column("integer")).errors, ["not_integer"]);
    assert.deepEqual(coerce(CELL.NUMBER, Infinity, column("integer")).errors, ["not_integer"]);
    const asText = coerce(CELL.STRING, "42", column("integer"));
    assert.equal(asText.value, 42);
    assert.deepEqual(asText.warnings, ["number_stored_as_text"]);
});

test("decimals: refused rather than rounded or overflowed", () => {
    assert.equal(coerce(CELL.NUMBER, 56.8663888889, column("numeric(18,10)")).value, 56.8663888889);
    assert.deepEqual(coerce(CELL.NUMBER, 0.3333333333333333, column("numeric(18,10)")).errors, ["too_many_decimals"]);
    assert.deepEqual(coerce(CELL.NUMBER, 1e9, column("numeric(18,10)")).errors, ["numeric_overflow"]);
    assert.equal(coerce(CELL.NUMBER, 99999999.5, column("numeric(18,10)")).value, 99999999.5);
    assert.deepEqual(coerce(CELL.NUMBER, 123456.1, column("numeric(10,5)")).errors, ["numeric_overflow"]);
    assert.deepEqual(coerce(CELL.NUMBER, 1e-11, column("numeric(18,10)")).errors, ["too_many_decimals"]);
    assert.deepEqual(coerce(CELL.NUMBER, NaN, column("numeric")).errors, ["not_finite"]);
    assert.deepEqual(coerce(CELL.NUMBER, Infinity, column("numeric(18,10)")).errors, ["not_finite"]);
    assert.equal(coerce(CELL.NUMBER, 0.3333333333333333, column("numeric")).value, 0.3333333333333333);
    assert.deepEqual(coerce(CELL.STRING, "1,5", column("numeric")).errors, ["decimal_comma"]);
});

test("text: length in characters, not UTF-16 units; NUL refused", () => {
    assert.equal(coerce(CELL.STRING, "\u{1F600}\u{1F600}\u{1F600}\u{1F600}\u{1F600}", column("character varying(5)")).value.length, 10);
    assert.deepEqual(coerce(CELL.STRING, "abcdef", column("character varying(5)")).errors, ["too_long"]);
    assert.deepEqual(coerce(CELL.STRING, "a\u0000b", column("text")).errors, ["nul_character"]);
});

test("tokens: only in key columns; elsewhere NEW- is text", () => {
    assert.equal(coerce(CELL.STRING, "NEW-1", column("integer"), true).value, "NEW-1");
    assert.deepEqual(coerce(CELL.STRING, "NEW-a b", column("integer"), true).errors, ["bad_token"]);
    assert.equal(coerce(CELL.STRING, "NEW-found layer", column("text")).value, "NEW-found layer");
    assert.equal(coerce(CELL.STRING, "new-york", column("text")).value, "new-york");
});
