/**
 * Reference vectors for the row hash, the canonical CSV and the machine-sheet
 * checksum (spec §9). An independent implementation must produce exactly these.
 * The row hash is SHA-256 over the RFC 8785 (JCS) serialisation of the
 * canonical values, first 16 hex characters; these vectors were cross-checked
 * against a separate Python implementation. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { rowHash, canonicalCsv, machineChecksum } from "../../src/Lib/SeadDataFormat/SdfCommon.js";

const ROW_HASHES = [
    [[], "4f53cda18c2baa0c"],
    [[null], "1d8fc6ceb1f94c63"],
    [[""], "1d8fc6ceb1f94c63"], //'' is NULL
    [[43187, 4312, null, 5, "0123", "2013-05-13T09:29:56.070308Z", null], "d93ea13c4a8a4169"],
    [[1, 0.1, 0.30000000000000004, 1e21, 5e-324, -12.5, 56.8663888889], "3aee09ead0a86fb2"],
    [[true, false, null], "4d0f18de21331182"],
    [["a\r\nb", "a\n\rb", "a\rb", "a\nb"], "4a21d7227004baa2"], //every line break is LF
    [["Slöinge Raä 114", "Café", "Café", "\u{1F600}", "tab\there", "quote\" and \\ backslash"], "ef038696aa8ff4e4"],
    [["  leading and trailing  ", "_x000D_", "NEW-1", "=1+1"], "c8e311aea6afa88a"],
];

test("row hashes", () => {
    for (const [values, hash] of ROW_HASHES) assert.equal(rowHash(values), hash, JSON.stringify(values));
});

test("canonical CSV", () => {
    assert.equal(canonicalCsv([["a", "b,c"], [null, true], ["line\nbreak", "quote\""]]), 'a,"b,c"\n,true\n"line\nbreak","quote"""\n');
});

test("machine-sheet checksum: _sdf_meta less its checksum row, then _sdf_columns, then _sdf_baseline", () => {
    const meta = [["key", "value"], ["sdf_version", "SDF/2.0"], ["checksum", "anything"]];
    const columns = [["sheet", "table"], ["sites", "tbl_sites"]];
    const baseline = [["table", "id", "hash", "shared"], ["tbl_sites", 1, "0123456789abcdef", "false"]];
    assert.equal(machineChecksum(meta, columns, baseline), "a7b64d730376d1b144edd76032878314f249e5a319536a1e37b8969dc31a93ee");
});
