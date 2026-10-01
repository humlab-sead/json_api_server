/**
 * Upload limits (review finding H13): what an uploaded workbook may unpack to,
 * and how many cells it may hold, is checked before ExcelJS parses it. The
 * limits are set tiny here, through the environment the reader reads them from.
 * Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import ExcelJS from "exceljs";

process.env.SDF_MAX_UNPACKED_MB = "1";
process.env.SDF_MAX_CELLS = "100"; //an import accepts 150
const { loadWorkbook } = await import("../../src/Lib/SeadDataFormat/SdfWorkbookReader.js");

async function workbook(fill) {
    const wb = new ExcelJS.Workbook();
    fill(wb.addWorksheet("t"));
    return Buffer.from(await wb.xlsx.writeBuffer());
}

test("a small workbook loads", async () => {
    const wb = await loadWorkbook(await workbook(ws => { ws.getCell("A1").value = "ok"; }));
    assert.equal(wb.getWorksheet("t").getCell("A1").value, "ok");
});

test("a workbook that unpacks past the limit is refused before it is parsed", async () => {
    //5 MB of one letter compresses to a few KB: a small upload, a large unpacking
    const buffer = await workbook(ws => { ws.getCell("A1").value = "a".repeat(5 * 1024 * 1024); });
    assert.ok(buffer.length < 100 * 1024, `upload is ${buffer.length} bytes`);
    await assert.rejects(loadWorkbook(buffer), err => err.code === "workbook_too_large");
});

test("a workbook with too many cells is refused before it is parsed", async () => {
    const buffer = await workbook(ws => { for (let r = 1; r <= 20; r++) for (let c = 1; c <= 10; c++) ws.getCell(r, c).value = r * c; });
    await assert.rejects(loadWorkbook(buffer), err => err.code === "workbook_too_large" && /200 cells/.test(err.message));
});
