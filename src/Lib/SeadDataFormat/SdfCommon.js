import crypto from "crypto";

/**
 * Shared definitions for the SEAD Data Format (plans/sead-data-format-design.html).
 *
 * Everything here is referenced by the specification by section number, because
 * these are the parts two independent implementations must agree on byte for
 * byte: the row hash (§9), the canonical CSV the checksum is taken over (§9),
 * and the cell text escaping (§8).
 */

export const SDF_VERSION = "SDF/2.0";

//Excel's hard limit on characters in a single cell (§8, OQ-21).
export const EXCEL_MAX_CELL_CHARS = 32767;

//An IEEE double holds any decimal of up to 15 significant digits exactly (§8).
export const MAX_EXACT_SIGNIFICANT_DIGITS = 15;

/**
 * A refusal to export, or a refusal of input. Always named (`code`) and always
 * specific (`detail`), because §8 and §10 require every failure to say what and
 * where rather than degrade silently.
 */
export class SdfError extends Error {
    constructor(code, message, detail = {}, statusCode = 422) {
        super(message);
        this.name = "SdfError";
        this.code = code;
        this.detail = detail;
        this.statusCode = statusCode;
    }
}

export function quoteIdent(name) {
    return `"${String(name).replace(/"/g, '""')}"`;
}

/**
 * The canonical form of one value for hashing (§9): NULL and '' are the same,
 * CRLF is compared as LF, and everything else is carried verbatim. Numbers are
 * JSON numbers, which JavaScript serialises as the shortest decimal that
 * round-trips through an IEEE double.
 */
export function canonicalValue(value) {
    if (value === null || value === undefined || value === "") {
        return null;
    }
    if (typeof value === "string") {
        return value.replace(/\r\n/g, "\n");
    }
    return value;
}

/**
 * Row hash (§9): SHA-256 over the UTF-8 JSON array of canonical values, first
 * 16 hex characters.
 */
export function rowHash(values) {
    const json = JSON.stringify(values.map(canonicalValue));
    return crypto.createHash("sha256").update(json, "utf8").digest("hex").slice(0, 16);
}

/**
 * Canonical CSV (§9): comma-separated, LF line endings including after the last
 * row, fields quoted only when they contain a comma, quote, CR or LF, quotes
 * doubled. null is an empty field; booleans are `true`/`false`.
 */
export function canonicalCsv(rows) {
    return rows.map(row => row.map(csvField).join(",")).join("\n") + "\n";
}

function csvField(value) {
    if (value === null || value === undefined) {
        return "";
    }
    const text = typeof value === "boolean" ? (value ? "true" : "false") : String(value);
    return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

export function sha256Hex(text) {
    return crypto.createHash("sha256").update(text, "utf8").digest("hex");
}

/**
 * Encodes text for a spreadsheet cell so that it survives exactly (§8).
 *
 * XML cannot carry most control characters, and an XML parser folds a raw CR
 * into LF, so a stored value containing either would come back changed. OOXML's
 * own escape, `_xHHHH_`, is what Excel writes for these and decodes on read.
 * A literal `_xHHHH_` already in the text is protected by escaping its
 * underscore as `_x005F_`, which is the same rule Excel applies.
 */
export function encodeCellText(text) {
    return text
        .replace(/_(x[0-9A-Fa-f]{4}_)/g, "_x005F_$1")
        .replace(/[\x00-\x08\x0B\x0C\x0D\x0E-\x1F\x7F]/g, ch =>
            `_x${ch.charCodeAt(0).toString(16).toUpperCase().padStart(4, "0")}_`);
}

/**
 * Significant digits in a decimal string as PostgreSQL renders numeric: no
 * exponent, optional sign and point. Leading zeros and trailing fractional
 * zeros are not significant.
 */
export function significantDigits(decimalText) {
    let digits = decimalText.replace(/^[+-]/, "");
    if (digits.includes(".")) {
        digits = digits.replace(/0+$/, "").replace(".", "");
    }
    else {
        digits = digits.replace(/0+$/, "");
    }
    digits = digits.replace(/^0+/, "");
    return digits.length;
}
