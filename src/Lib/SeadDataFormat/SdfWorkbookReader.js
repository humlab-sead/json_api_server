import ExcelJS from "exceljs";
import JSZip from "jszip";
import { SdfError, DEFAULT_MAX_CELLS } from "./SdfCommon.js";

//An .xlsx is a zip, so a few MB can unpack to gigabytes. What an upload may unpack
//to, and how many cells it may hold, is checked before anything is parsed. An
//exported workbook holds its data cells (bounded by SDF_MAX_CELLS at export) plus
//labels, lists and machine sheets, hence the margin.
const MAX_UNPACKED_BYTES = (parseInt(process.env.SDF_MAX_UNPACKED_MB) || 256) * 1024 * 1024;
const MAX_CELLS = Math.round((parseInt(process.env.SDF_MAX_CELLS) || DEFAULT_MAX_CELLS) * 1.5);

/**
 * Loading an uploaded workbook for import (spec §10 stage 1), and reading its
 * cells by stored type (§8: the importer reads what a cell is, not what it
 * displays).
 */

export const CELL = {
    BLANK: "blank",
    NUMBER: "number",
    STRING: "string",
    BOOLEAN: "boolean",
    DATE: "date",
    FORMULA: "formula",
    ERROR: "error",
};

/**
 * Loads an .xlsx for reading. Anything that is not one is refused with a named
 * error, CSV in particular (§8: only .xlsx is input).
 *
 * Data validations, conditional formatting and extension lists are removed
 * before parsing. The importer never reads them, and ExcelJS's reader expands a
 * validated range into one entry per cell, so a curator who extends a dropdown
 * to a whole column would otherwise make the file unreadable.
 *
 * Escapes are decoded case-insensitively (§8): LibreOffice writes `_x000b_`
 * where Excel writes `_x000B_`, and ExcelJS only decodes the latter, so the hex
 * of every escape is upper-cased first, in one left-to-right pass.
 *
 * Every entry is inflated as a stream against a byte budget, and the cells are
 * counted, before ExcelJS parses anything, so a crafted file is refused without
 * exhausting memory.
 */
export async function loadWorkbook(buffer) {
    const looksLikeZip = buffer.length > 4 && buffer[0] === 0x50 && buffer[1] === 0x4b;
    if (!looksLikeZip) {
        const head = buffer.slice(0, 2048).toString("utf8");
        const csvLike = /[,;\t]/.test(head) && !/[\x00-\x08]/.test(head);
        throw new SdfError(csvLike ? "csv_not_accepted" : "not_xlsx",
            csvLike
                ? "This looks like a CSV file. CSV cannot be imported, because it does not keep values like 0123 or 20-30 intact. Upload the .xlsx workbook you downloaded from SEAD."
                : "This is not an .xlsx workbook. Upload the .xlsx file you downloaded from SEAD.",
            {}, 422);
    }

    let zip;
    try {
        zip = await JSZip.loadAsync(buffer);
    }
    catch (err) {
        throw new SdfError("not_xlsx", "The file could not be opened as an .xlsx workbook. It may be damaged.", {}, 422);
    }
    if (!zip.file("xl/workbook.xml")) {
        throw new SdfError("not_xlsx", "This is not an .xlsx workbook (it may be another kind of zip file).", {}, 422);
    }

    const texts = new Map();
    let budget = MAX_UNPACKED_BYTES;
    let cells = 0;
    for (const [path, file] of Object.entries(zip.files)) {
        if (file.dir) continue;
        const data = await inflate(file, budget);
        budget -= data.length;
        const isSheet = /^xl\/worksheets\/[^/]+\.xml$/.test(path);
        if (isSheet || path === "xl/sharedStrings.xml") texts.set(path, data.toString("utf8"));
        if (isSheet) cells += (texts.get(path).match(/<c[\s>]/g) || []).length;
    }
    if (cells > MAX_CELLS) {
        throw new SdfError("workbook_too_large",
            `The workbook has ${cells.toLocaleString("en")} cells, more than the ${MAX_CELLS.toLocaleString("en")} an import accepts. Split the work into exports of fewer sites.`,
            { cells, maxCells: MAX_CELLS }, 413);
    }

    for (const [path, xml] of texts) {
        const isSheet = path !== "xl/sharedStrings.xml";
        let patched = xml.replace(/_x([0-9A-Fa-f]{4})_/g, (m, hex) => `_x${hex.toUpperCase()}_`);
        if (isSheet) {
            patched = patched
                .replace(/<dataValidations\b[\s\S]*?<\/dataValidations>/g, "")
                .replace(/<conditionalFormatting\b[\s\S]*?<\/conditionalFormatting>/g, "")
                .replace(/<extLst\b[\s\S]*?<\/extLst>/g, "");
        }
        if (patched !== xml) zip.file(path, patched);
    }

    const wb = new ExcelJS.Workbook();
    try {
        await wb.xlsx.load(await zip.generateAsync({ type: "nodebuffer" }));
    }
    catch (err) {
        throw new SdfError("not_xlsx", `The workbook could not be read: ${err.message}`, {}, 422);
    }
    return wb;
}

/**
 * One zip entry, inflated as a stream and abandoned as soon as it passes the
 * byte budget.
 */
function inflate(file, budget) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        let size = 0;
        const stream = file.internalStream("nodebuffer");
        stream
            .on("data", chunk => {
                size += chunk.length;
                if (size > budget) {
                    stream.pause();
                    reject(new SdfError("workbook_too_large",
                        `The workbook unpacks to more than the ${MAX_UNPACKED_BYTES / 1024 / 1024} MB an import accepts.`, {}, 413));
                    return;
                }
                chunks.push(chunk);
            })
            .on("error", err => reject(new SdfError("not_xlsx", `The workbook could not be unpacked: ${err.message}`, {}, 422)))
            .on("end", () => resolve(Buffer.concat(chunks)))
            .resume();
    });
}

/**
 * A cell as { kind, value, address }. Rich text and hyperlinks read as their
 * text. Formulas are reported as formulas whatever their cached result, since
 * §8 does not trust the cache.
 */
export function readCell(cell) {
    const address = cell.address;
    const v = cell.value;
    switch (cell.type) {
        case ExcelJS.ValueType.Null:
        case ExcelJS.ValueType.Merge:
            return { kind: CELL.BLANK, value: null, address };
        case ExcelJS.ValueType.Number:
            return { kind: CELL.NUMBER, value: v, address };
        case ExcelJS.ValueType.String:
        case ExcelJS.ValueType.SharedString:
            return v === "" ? { kind: CELL.BLANK, value: null, address } : { kind: CELL.STRING, value: v, address };
        case ExcelJS.ValueType.RichText: {
            const text = v.richText.map(r => r.text).join("");
            return text === "" ? { kind: CELL.BLANK, value: null, address } : { kind: CELL.STRING, value: text, address };
        }
        case ExcelJS.ValueType.Hyperlink: {
            //the text of a link can itself be rich text
            const text = typeof v.text === "string" ? v.text
                : v.text && Array.isArray(v.text.richText) ? v.text.richText.map(r => r.text).join("")
                : String(v.text ?? "");
            return text === "" ? { kind: CELL.BLANK, value: null, address } : { kind: CELL.STRING, value: text, address };
        }
        case ExcelJS.ValueType.Date:
            return { kind: CELL.DATE, value: v, address };
        case ExcelJS.ValueType.Boolean:
            return { kind: CELL.BOOLEAN, value: v, address };
        case ExcelJS.ValueType.Formula: {
            const formula = v.formula || v.sharedFormula || "";
            //LibreOffice stores every boolean as the formula TRUE() or FALSE();
            //those two are constants, not calculations
            const constant = /^\s*(TRUE|FALSE)\s*\(\s*\)\s*$/i.exec(formula);
            if (constant) return { kind: CELL.BOOLEAN, value: constant[1].toUpperCase() === "TRUE", address };
            return { kind: CELL.FORMULA, value: formula, address };
        }
        case ExcelJS.ValueType.Error:
            return { kind: CELL.ERROR, value: v && v.error, address };
        default:
            return { kind: CELL.STRING, value: String(v), address };
    }
}

/** A cell's plain value for machine sheets and headers: blank is null. */
export function plainValue(cell) {
    const c = readCell(cell);
    return c.kind === CELL.BLANK ? null : c.value;
}

/**
 * The rows of a worksheet as arrays of plain values, header row included,
 * trailing blank rows dropped. For the machine sheets.
 */
export function plainRows(ws) {
    const width = ws.columnCount;
    const rows = [];
    ws.eachRow({ includeEmpty: true }, (row, rowNumber) => {
        const values = [];
        for (let c = 1; c <= width; c++) values.push(plainValue(row.getCell(c)));
        rows[rowNumber - 1] = values;
    });
    for (let i = 0; i < rows.length; i++) {
        if (!rows[i]) rows[i] = new Array(width).fill(null);
    }
    while (rows.length && rows[rows.length - 1].every(v => v === null)) rows.pop();
    //a sheet's width can exceed its data; trim columns that are blank throughout
    let used = 0;
    for (const r of rows) r.forEach((v, i) => { if (v !== null) used = Math.max(used, i + 1); });
    return rows.map(r => r.slice(0, used));
}
