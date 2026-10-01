import ExcelJS from "exceljs";
import JSZip from "jszip";
import { encodeCellText } from "./SdfCommon.js";
import { buildGuide } from "./SdfGuide.js";

/**
 * Writes an SDF structure (SdfExporter) to .xlsx (spec §2, the Renderer role).
 *
 * The renderer must not change a value, header, column order or sheet order.
 * Everything it adds - the README, header notes, colours, dropdowns,
 * highlighting - is the §11 legibility layer, which the importer ignores.
 */

//The colour legend. SdfGuide section 4 describes these to the user.
export const COLOURS = {
    keyHeader: "FF9BC2E6", key: "FFDDEBF7",           //light blue: ids
    readonlyHeader: "FFBFBFBF", readonly: "FFEDEDED", //grey: labels, date_updated
    actionHeader: "FFF4B183", action: "FFFCE4D6",     //amber: _action
    dataHeader: "FFF2F2F2",
    newRow: "FFC6EFCE",                               //green
    deleteRow: "FFFFC7CE",                            //red
    referenceTab: "FF808080",
    readmeTab: "FF2F75B5",
};

const EXCEL_MAX_ROW = 1048576;
//Data validations cover the data rows plus this many spare rows for additions.
//Not the whole column: ExcelJS, which the importer reads with, expands a
//validation range into one entry per cell when reading, so a whole-column
//range costs a million entries per column.
const VALIDATION_SPARE_ROWS = 1000;
const LISTS_SHEET = "_sdf_lists";

export default class SdfRenderer {

    constructor(guideConfig) {
        this.guideConfig = guideConfig;
    }

    async render(bundle) {
        const meta = new Map(bundle.meta);
        const wb = new ExcelJS.Workbook();
        wb.creator = meta.get("exporter");
        wb.created = new Date(meta.get("exported_at"));
        wb.title = `SEAD site export ${meta.get("site_ids")}`;

        this._renderReadme(wb, bundle);

        const listRanges = this._listRanges(bundle.lists);
        for (const sheet of bundle.sheets) {
            this._renderDataSheet(wb, sheet, listRanges);
        }

        this._renderLists(wb, bundle.lists);
        this._renderMachineSheet(wb, "_sdf_meta", [["key", "value"], ...bundle.meta], [true, true]);
        this._renderMachineSheet(wb, "_sdf_columns", bundle.machine.columns, bundle.machine.columns[0].map(() => true));
        this._renderMachineSheet(wb, "_sdf_baseline", bundle.machine.baseline, [true, false, true, true]);

        const buffer = await wb.xlsx.writeBuffer();
        return await addQuotePrefix(buffer);
    }

    //------------------------------------------------------------ data sheets

    _renderDataSheet(wb, sheet, listRanges) {
        const isReference = sheet.role !== "owned";
        const ws = wb.addWorksheet(sheet.name, {
            views: [{ state: "frozen", ySplit: 1 }],
            properties: isReference ? { tabColor: { argb: COLOURS.referenceTab } } : {},
        });

        ws.columns = sheet.columns.map((c, i) => ({
            header: c.key,
            key: c.key,
            width: this._width(c, sheet.rows, i),
            style: this._columnStyle(c),
        }));

        const header = ws.getRow(1);
        sheet.columns.forEach((c, i) => {
            const cell = header.getCell(i + 1);
            cell.value = c.key; //verbatim, never a friendly title (§5)
            cell.font = { bold: true };
            cell.fill = solid(this._category(c) + "Header", "dataHeader");
            cell.numFmt = "@";
            cell.note = this._headerNote(sheet, c);
        });

        const kinds = sheet.columns.map(c => c.valueKind);
        for (const row of sheet.rows) {
            ws.addRow(row.map((value, i) => cellValue(value, kinds[i])));
        }

        const lastCol = ws.getColumn(sheet.columns.length).letter;
        ws.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1, column: sheet.columns.length } };

        const lastValidatedRow = sheet.rows.length + 1 + VALIDATION_SPARE_ROWS;

        //_action: a delete dropdown
        ws.dataValidations.add(`A2:A${lastValidatedRow}`, {
            type: "list", allowBlank: true, formulae: ['"delete"'],
            showErrorMessage: true, errorStyle: "stop",
            errorTitle: "_action", error: "Leave empty, or type delete.",
        });

        sheet.columns.forEach((c, i) => {
            const letter = ws.getColumn(i + 1).letter;
            const range = `${letter}2:${letter}${lastValidatedRow}`;
            //label columns pointing at a fully shipped list get that list as a dropdown
            if (c.kind === "label" && listRanges.has(c.labelTarget)) {
                ws.dataValidations.add(range, {
                    type: "list", allowBlank: true, formulae: [listRanges.get(c.labelTarget)],
                    showErrorMessage: true, errorStyle: "warning",
                    errorTitle: c.key, error: "Not a value from the list. It will be checked on import.",
                });
            }
            //numeric data columns: catch most locale mistakes at the keyboard (§8).
            //Key columns are left alone, since they also accept NEW- tokens.
            if (c.kind === "data" && !c.isPk && !c.fkTable && (c.valueKind === "integer" || c.valueKind === "decimal")) {
                ws.dataValidations.add(range, {
                    type: c.valueKind === "integer" ? "whole" : "decimal",
                    operator: "between", allowBlank: true,
                    formulae: [-1e15, 1e15],
                    showErrorMessage: true, errorStyle: "stop",
                    errorTitle: c.key, error: c.valueKind === "integer" ? "Enter a whole number." : "Enter a number.",
                });
            }
        });

        const pkIndex = sheet.columns.findIndex(c => c.isPk);
        const pkLetter = ws.getColumn(pkIndex + 1).letter;
        ws.addConditionalFormatting({
            ref: `A2:${lastCol}${EXCEL_MAX_ROW}`,
            rules: [
                {
                    type: "expression", priority: 1, formulae: ['$A2="delete"'],
                    style: { fill: { type: "pattern", pattern: "solid", bgColor: { argb: COLOURS.deleteRow } } },
                },
                {
                    type: "expression", priority: 2, formulae: [`LEFT($${pkLetter}2,4)="NEW-"`],
                    style: { fill: { type: "pattern", pattern: "solid", bgColor: { argb: COLOURS.newRow } } },
                },
            ],
        });
    }

    _category(column) {
        if (column.kind === "action") return "action";
        if (column.kind === "label" || column.kind === "system") return "readonly";
        if (column.isPk || column.fkTable) return "key";
        return "data";
    }

    _columnStyle(column) {
        const style = {};
        if (column.valueKind === "text") style.numFmt = "@";
        else if (column.valueKind === "integer") style.numFmt = "0";
        const category = this._category(column);
        if (category !== "data") style.fill = solid(category);
        return style;
    }

    _headerNote(sheet, column) {
        const lines = [];
        if (column.kind === "action") {
            lines.push("Action", sheet.role === "reference"
                ? "Leave empty, or type delete to suggest removing this entry from the shared list."
                : "Leave empty. Deleting rows is not supported yet. Rows missing from the sheet are never deleted.");
            return lines.join("\n");
        }
        lines.push(friendlyTitle(column.key));
        if (column.comment) lines.push(column.comment);
        if (column.pgType) lines.push(`Type: ${column.pgType}${column.nullable === false ? ", required" : ""}.`);
        if (column.kind === "system") {
            lines.push("Maintained by the database. Ignored on import.");
        }
        else if (column.kind === "label") {
            //the comment already says what the label is for
        }
        else if (sheet.role === "reference") {
            lines.push("Shared list: changes here become suggestions for a data manager to review.");
        }
        else if (sheet.role === "reference-deprecated") {
            lines.push("Legacy table, read-only: changes are reported but never applied.");
        }
        else {
            lines.push("Read back on import.");
        }
        return lines.join("\n");
    }

    _width(column, rows, index) {
        let longest = column.key.length + 2;
        const sample = Math.min(rows.length, 300);
        for (let r = 0; r < sample; r++) {
            const v = rows[r][index];
            if (v === null || v === undefined) continue;
            const text = String(v);
            const firstLine = text.split("\n", 1)[0];
            longest = Math.max(longest, Math.min(firstLine.length, 60));
        }
        return Math.max(8, Math.min(longest + 1, 50));
    }

    //------------------------------------------------------------- lists

    /**
     * Dropdown sources for label columns pointing at fully shipped reference
     * tables: one column per table on a hidden sheet.
     */
    _listRanges(lists) {
        const ranges = new Map();
        lists.forEach((list, i) => {
            if (!list.values.length) return;
            const letter = columnLetter(i + 1);
            ranges.set(list.table, `'${LISTS_SHEET}'!$${letter}$2:$${letter}$${list.values.length + 1}`);
        });
        return ranges;
    }

    _renderLists(wb, lists) {
        const ws = wb.addWorksheet(LISTS_SHEET, { state: "hidden" });
        lists.forEach((list, i) => {
            const col = ws.getColumn(i + 1);
            col.numFmt = "@";
            ws.getCell(1, i + 1).value = list.sheet;
            list.values.forEach((value, r) => {
                ws.getCell(r + 2, i + 1).value = value === null ? null : encodeCellText(value);
            });
        });
    }

    //---------------------------------------------------------- machine sheets

    _renderMachineSheet(wb, name, rows, textColumns) {
        const ws = wb.addWorksheet(name, { state: "hidden" });
        textColumns.forEach((isText, i) => {
            if (isText) ws.getColumn(i + 1).numFmt = "@";
        });
        //Booleans are written as the text true/false. The checksum (§9) reads them
        //that way regardless, and LibreOffice rewrites boolean cells as formulas.
        for (const row of rows) {
            ws.addRow(row.map(v => {
                if (typeof v === "boolean") return v ? "true" : "false";
                return typeof v === "string" ? encodeCellText(v) : v ?? null;
            }));
        }
        ws.getRow(1).font = { bold: true };
    }

    //----------------------------------------------------------------- README

    _renderReadme(wb, bundle) {
        const ws = wb.addWorksheet("README", { properties: { tabColor: { argb: COLOURS.readmeTab } } });
        ws.getColumn(1).width = 38;
        ws.getColumn(2).width = 100;
        ws.getColumn(3).width = 60;
        ws.getColumn(4).width = 26;

        const wrap = { wrapText: true, vertical: "top" };
        for (const block of buildGuide(bundle, this.guideConfig)) {
            if (block.type === "title") {
                const row = ws.addRow([block.text]);
                row.font = { bold: true, size: 16 };
                ws.addRow([]);
            }
            else if (block.type === "section") {
                ws.addRow([]);
                const row = ws.addRow([block.text]);
                row.font = { bold: true, size: 13 };
                row.getCell(1).border = { bottom: { style: "thin" } };
                row.getCell(2).border = { bottom: { style: "thin" } };
            }
            else if (block.type === "row") {
                const row = ws.addRow([block.label, block.text]);
                row.getCell(1).alignment = wrap;
                row.getCell(2).alignment = wrap;
                if (block.strong) row.getCell(1).font = { bold: true };
                if (block.legend) row.getCell(1).fill = solid(block.legend);
            }
            else if (block.type === "paragraph") {
                const row = ws.addRow(["", block.text]);
                row.getCell(2).alignment = wrap;
            }
            else if (block.type === "table") {
                const head = ws.addRow(block.header);
                head.font = { bold: true };
                head.eachCell(cell => { cell.fill = solid("dataHeader"); });
                for (const r of block.rows) {
                    const row = ws.addRow(r.map(v => (typeof v === "string" ? encodeCellText(v) : v)));
                    row.eachCell(cell => { cell.alignment = wrap; });
                }
            }
        }
    }
}

/**
 * §8 requires quotePrefix on every text-formatted cell: it suppresses the
 * "number stored as text" warning that invites a user to convert 0123 or 20-30
 * into a number. ExcelJS cannot write the attribute, so it is added to every
 * cell format using the built-in Text format (id 49) in styles.xml.
 */
export async function addQuotePrefix(buffer) {
    const zip = await JSZip.loadAsync(buffer);
    const path = "xl/styles.xml";
    const xml = await zip.file(path).async("string");
    const patched = xml.replace(/<cellXfs\b[^>]*>[\s\S]*?<\/cellXfs>/, block =>
        block.replace(/<xf\b([^>]*?)(\/?)>/g, (tag, attrs, selfClose) =>
            /\bnumFmtId="49"/.test(attrs) && !/\bquotePrefix=/.test(attrs)
                ? `<xf${attrs} quotePrefix="1"${selfClose}>`
                : tag));
    zip.file(path, patched);
    return await zip.generateAsync({ type: "nodebuffer", compression: "DEFLATE", compressionOptions: { level: 6 } });
}

function solid(category, fallback) {
    const argb = COLOURS[category] || COLOURS[fallback];
    return { type: "pattern", pattern: "solid", fgColor: { argb } };
}

/**
 * The value written to a cell for a carrier value (§8). Text is escaped so
 * control characters and carriage returns survive; an empty string is written
 * blank, since a spreadsheet cannot tell it from NULL (§8, OQ-25).
 */
function cellValue(value, valueKind) {
    if (value === null || value === undefined || value === "") {
        return null;
    }
    if (valueKind === "text" && typeof value === "string") {
        return encodeCellText(value);
    }
    return value;
}

function friendlyTitle(key) {
    const text = key.replace(/_/g, " ").trim();
    return text.charAt(0).toUpperCase() + text.slice(1);
}

function columnLetter(n) {
    let s = "";
    while (n > 0) {
        const m = (n - 1) % 26;
        s = String.fromCharCode(65 + m) + s;
        n = Math.floor((n - 1) / 26);
    }
    return s;
}
