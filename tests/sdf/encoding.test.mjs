/**
 * Cell text encoding (spec §8): what SDF writes must read back exactly, through
 * ExcelJS and, as far as LibreOffice allows, through a LibreOffice re-save.
 * Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import ExcelJS from "exceljs";
import { encodeCellText, canonicalValue, ESCAPE_LIKE } from "../../src/Lib/SeadDataFormat/SdfCommon.js";
import { loadWorkbook, readCell } from "../../src/Lib/SeadDataFormat/SdfWorkbookReader.js";

const ALPHABET = ["_", "x", "X", "0", "D", "5", "F", "f", "b", "B", "\r", "\n", "\r\n", "\x01", "\x0b", "a", " ", "￿", "￾", "\uD800", "\uDC00"];

/** A deterministic set of adversarial strings (mulberry32). */
function fuzzStrings(count, seed = 20261001) {
    let a = seed;
    const random = () => {
        a |= 0; a = (a + 0x6D2B79F5) | 0;
        let t = Math.imul(a ^ (a >>> 15), 1 | a);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
    const out = [];
    for (let i = 0; i < count; i++) {
        const length = 1 + Math.floor(random() * 12);
        let text = "";
        for (let j = 0; j < length; j++) text += ALPHABET[Math.floor(random() * ALPHABET.length)];
        out.push(text);
    }
    return out;
}

const NAMED = [
    "lone\rCR", "trailing CR\r", "CRLF\r\nline", "LF CR\n\rline", "a\x01b", "a\x0bb", "a\x1fb", "a\x7fb",
    "_x000D_ literal", "_x005F_", "_x005F_x000D_", "_x000D_x000A_", "_x000D\r", "_X000d_", "_xFFFF_",
    "￾", "￿", "lone \uD800 high", "lone \uDC00 low", "pair 😀 ok", "0123", "20-30", "=1+1", "NEW-1",
];

async function writeAndRead(strings) {
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("t");
    strings.forEach((s, i) => { ws.getCell(i + 1, 1).value = encodeCellText(s); });
    return readBack(Buffer.from(await wb.xlsx.writeBuffer()), strings.length);
}

async function readBack(buffer, count) {
    const wb = await loadWorkbook(buffer);
    const ws = wb.getWorksheet("t");
    const out = [];
    for (let i = 1; i <= count; i++) out.push(readCell(ws.getCell(i, 1)).value);
    return out;
}

test("canonical form: every line-break form is one LF; empty is null", () => {
    assert.equal(canonicalValue("a\r\nb"), "a\nb");
    assert.equal(canonicalValue("a\n\rb"), "a\nb");
    assert.equal(canonicalValue("a\rb"), "a\nb");
    assert.equal(canonicalValue("a\r\n\rb"), "a\n\nb");
    assert.equal(canonicalValue(""), null);
    assert.equal(canonicalValue(null), null);
    assert.equal(canonicalValue(1.5), 1.5);
});

test("named tricky strings survive an ExcelJS round trip exactly", async () => {
    const back = await writeAndRead(NAMED);
    NAMED.forEach((s, i) => assert.equal(back[i], s, `#${i} ${JSON.stringify(s)}`));
});

test("3,000 adversarial strings survive an ExcelJS round trip exactly", async () => {
    const strings = fuzzStrings(3000);
    const back = await writeAndRead(strings);
    const differ = strings.filter((s, i) => back[i] !== s);
    assert.deepEqual(differ, []);
});

test("lowercase escapes, as LibreOffice writes them, are decoded", async () => {
    //ExcelJS writes text verbatim, so this workbook carries _x000b_ the way LibreOffice does
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("t");
    ws.getCell(1, 1).value = "a_x000b_b";
    ws.getCell(2, 1).value = "_x005f_x000b_";
    const back = await readBack(Buffer.from(await wb.xlsx.writeBuffer()), 2);
    assert.equal(back[0], "a\x0bb");
    assert.equal(back[1], "_x000b_");
});

const soffice = (() => {
    try { execFileSync("soffice", ["--version"], { stdio: "ignore" }); return true; }
    catch { return false; }
})();

test("a LibreOffice re-save changes only text LibreOffice itself mis-encodes", { skip: !soffice && "LibreOffice is not installed" }, async () => {
    //Lone surrogates are left out: PostgreSQL stores valid UTF-8 only, so no
    //database value holds one, and LibreOffice does not keep them.
    const loneSurrogate = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;
    const strings = [...NAMED, ...fuzzStrings(3000)].filter(s => !loneSurrogate.test(s));
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("t");
    ws.getColumn(1).numFmt = "@";
    strings.forEach((s, i) => { ws.getCell(i + 1, 1).value = encodeCellText(s); });
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "sdf-lo-"));
    try {
        const input = path.join(dir, "in.xlsx");
        fs.writeFileSync(input, Buffer.from(await wb.xlsx.writeBuffer()));
        const outDir = path.join(dir, "out");
        execFileSync("soffice", [`-env:UserInstallation=file://${dir}/profile`, "--headless", "--convert-to", "xlsx", "--outdir", outDir, input],
            { stdio: "ignore", timeout: 180000 });
        const back = await readBack(fs.readFileSync(path.join(outDir, "in.xlsx")), strings.length);
        const differ = strings.filter((s, i) => canonicalValue(back[i]) !== canonicalValue(s));
        //what remains is text with overlapping escape-like sequences, which
        //LibreOffice's own writer mis-encodes (§8)
        assert.deepEqual(differ.filter(s => !ESCAPE_LIKE.test(s)), []);
        assert.ok(differ.length <= 10, `${differ.length} strings changed: ${JSON.stringify(differ.slice(0, 10))}`);
    }
    finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});
