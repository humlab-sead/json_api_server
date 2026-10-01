import {
    EXCEL_MAX_CELL_CHARS, MAX_EXACT_SIGNIFICANT_DIGITS, SdfError, quoteIdent, significantDigits,
} from "./SdfCommon.js";

/**
 * Reading rows in their §8 carrier form. Shared by the exporter and the
 * validator: the validator's live rows must be read exactly as the export was,
 * or the three-way comparison would see differences that are only encoding.
 */

/**
 * The select list for a table, one expression per exported column, cast so that
 * every value arrives in its carrier form: integers and decimals as text
 * (converted and checked by normaliseRow, since int8 and numeric exceed what the
 * driver returns as numbers), dates and timestamps as ISO strings, booleans as
 * booleans, everything else as text.
 */
export function selectList(schema, table) {
    return schema.exportedColumns(table)
        .map(col => `${carrierExpression(col, `t.${quoteIdent(col.name)}`)} as ${quoteIdent(col.name)}`)
        .join(", ");
}

/**
 * The SQL expression giving a value of this column in its carrier form. The
 * validator puts typed-in text through the same expression (§8), so what a
 * curator types and what the database holds are compared in one form.
 */
export function carrierExpression(col, q) {
    switch (col.typname) {
        case "int2": case "int4": case "bool":
            return q;
        case "date":
            return `to_char(${q}, 'YYYY-MM-DD')`;
        case "timestamptz":
            return `to_char(${q} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;
        case "timestamp":
            return `to_char(${q}, 'YYYY-MM-DD"T"HH24:MI:SS.US')`;
        default:
            return `${q}::text`;
    }
}

/** Text-carried types whose text PostgreSQL normalises: dates, UUIDs, ranges, … */
export function isNormalisedText(col) {
    return col.valueKind === "text" && !["text", "varchar", "bpchar"].includes(col.typname);
}

export async function fetchRows(client, schema, table, where, params) {
    const sql = `select ${selectList(schema, table)} from public.${quoteIdent(table.name)} t
                 where ${where} order by t.${quoteIdent(table.pk)}`;
    const res = await client.query(sql, params);
    return res.rows.map(row => normaliseRow(schema, table, row));
}

/** The array type to bind a list of values for one column: `int4[]`, `text[]`, … */
export function arrayType(schema, table, columnName) {
    return `${schema.column(table, columnName).typname}[]`;
}

/**
 * Converts a fetched row to its carrier values, refusing anything a cell cannot
 * hold exactly (§8): a decimal beyond 15 significant digits, an integer beyond
 * 2^53, a non-finite number, or text over Excel's cell limit.
 */
export function normaliseRow(schema, table, row) {
    for (const col of schema.exportedColumns(table)) {
        const value = row[col.name];
        if (value === null || value === undefined) {
            row[col.name] = null;
            continue;
        }
        const where = () => ({ table: table.name, column: col.name, id: row[table.pk] });
        if (col.valueKind === "integer") {
            const n = Number(value);
            if (!Number.isSafeInteger(n)) {
                throw new SdfError("value_not_representable",
                    `${table.name}.${col.name} holds ${value}, beyond the integers a spreadsheet stores exactly.`, where());
            }
            row[col.name] = n;
        }
        else if (col.valueKind === "decimal") {
            const n = Number(value);
            if (!Number.isFinite(n)) {
                throw new SdfError("value_not_representable",
                    `${table.name}.${col.name} holds ${value}, which a spreadsheet cannot store.`, where());
            }
            if (col.typname === "numeric" && significantDigits(value) > MAX_EXACT_SIGNIFICANT_DIGITS) {
                throw new SdfError("value_not_representable",
                    `${table.name}.${col.name} holds ${value}, more than ${MAX_EXACT_SIGNIFICANT_DIGITS} significant digits.`, where());
            }
            row[col.name] = n;
        }
        else if (col.valueKind === "text" && value.length > EXCEL_MAX_CELL_CHARS) {
            throw new SdfError("text_too_long",
                `${table.name}.${col.name} holds ${value.length} characters; a spreadsheet cell holds at most ${EXCEL_MAX_CELL_CHARS}.`,
                where());
        }
    }
    return row;
}
