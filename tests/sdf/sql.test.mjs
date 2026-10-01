/**
 * SQL literals in generated change requests (spec §10). Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import SdfChangeRequest from "../../src/Lib/SeadDataFormat/SdfChangeRequest.class.js";

const generator = new SdfChangeRequest({});
const columns = {
    v: { name: "v", baseType: "numeric", valueKind: "decimal" },
    n: { name: "n", baseType: "character varying", valueKind: "text" },
};
const schema = { table: () => ({ name: "t" }), column: (t, name) => columns[name] };
const literal = (column, value, exact) => generator._literal(schema, "t", column, value, exact);

test("decimals are written with the database's own text where it holds the same number", () => {
    assert.equal(literal("v", 12.5, "12.50"), "12.50::numeric");
    assert.equal(literal("v", -12.5, "-12.50"), "(-12.50)::numeric");
    assert.equal(literal("v", 1e-7, "0.0000001"), "0.0000001::numeric");
    assert.equal(literal("v", 12.5), "12.5::numeric");
    assert.equal(literal("v", 13, "12.50"), "13::numeric"); //a different number: the exact text does not apply
});

test("text is cast to the base type, so over-long values raise instead of truncating", () => {
    assert.equal(literal("n", "abc"), "'abc'::character varying");
    assert.equal(literal("n", "it's"), "'it''s'::character varying");
    assert.equal(literal("n", "a\nb"), "E'a\\nb'::character varying");
    assert.equal(literal("n", null), "null");
});
