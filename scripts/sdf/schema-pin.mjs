/**
 * The pinned SDF schema model (plans/sdf-review-report.md, test plan item 2).
 *
 * SdfSchema derives everything an export contains from pg_catalog: which tables
 * are site data (owned) and how each is reached, which are shared lists and how
 * they are shipped, and the sheet names. A schema change through change control
 * can therefore change what SDF exports without any change here. This script
 * compares the model against schema-pin.json and fails, listing every difference,
 * when they disagree, so such a change is noticed and reviewed. It also fails on
 * sheet names a spreadsheet cannot hold (longer than 31 characters, or equal to
 * another ignoring case).
 *
 * Read-only. From the json_api_server directory:
 *   node scripts/sdf/schema-pin.mjs            compare
 *   node scripts/sdf/schema-pin.mjs --update   rewrite schema-pin.json after a reviewed change
 * Connects with POSTGRES_* (as json_api_server does), or libpq's PG* variables.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import pkg from "pg";
import SdfSchema from "../../src/Lib/SeadDataFormat/SdfSchema.class.js";

const PIN = path.join(path.dirname(fileURLToPath(import.meta.url)), "schema-pin.json");
const EXCEL_MAX_SHEET_NAME = 31;

const env = process.env;
const pool = new pkg.Pool(env.POSTGRES_HOST ? {
    user: env.POSTGRES_USER, host: env.POSTGRES_HOST, database: env.POSTGRES_DATABASE,
    password: env.POSTGRES_PASS, port: env.POSTGRES_PORT,
} : {});

const client = await pool.connect();
let model;
try {
    await client.query("begin isolation level repeatable read read only");
    const schema = await SdfSchema.load(client);
    model = {
        owned: Object.fromEntries([...schema.owned.values()]
            .sort((a, b) => a.table.name.localeCompare(b.table.name))
            .map(e => [e.table.name, {
                sheet: e.table.sheet,
                reach: e.reach,
                ...(e.deprecated ? { deprecated: e.deprecated } : {}),
                ownership_keys: schema.ownershipKeys(e.table.name).map(fk => `${fk.column} -> ${fk.parent}`).sort(),
            }])),
        referenced: Object.fromEntries([...schema.referenced.values()]
            .sort((a, b) => a.table.name.localeCompare(b.table.name))
            .map(e => [e.table.name, { sheet: e.table.sheet, mode: e.mode, ...(e.deprecated ? { deprecated: e.deprecated } : {}) }])),
        sheet_order: schema.ownedOrder.map(n => schema.table(n).sheet),
    };
    await client.query("rollback");
}
finally {
    client.release();
    await pool.end();
}

const problems = [];
const sheets = [...Object.values(model.owned), ...Object.values(model.referenced)].map(t => t.sheet);
const reserved = ["readme", "_sdf_meta", "_sdf_columns", "_sdf_baseline", "_sdf_lists"];
const seen = new Map();
for (const sheet of sheets) {
    if (sheet.length > EXCEL_MAX_SHEET_NAME) problems.push(`sheet name "${sheet}" is ${sheet.length} characters; a spreadsheet holds ${EXCEL_MAX_SHEET_NAME}`);
    const key = sheet.toLowerCase();
    if (reserved.includes(key) || key.startsWith("view_") || key.startsWith("readme_") || key.startsWith("scratch")) problems.push(`sheet name "${sheet}" collides with a reserved name`);
    if (seen.has(key)) problems.push(`sheet names "${seen.get(key)}" and "${sheet}" differ only in case`);
    seen.set(key, sheet);
}

if (process.argv.includes("--update")) {
    fs.writeFileSync(PIN, JSON.stringify(model, null, 2) + "\n");
    console.log(`Wrote ${path.relative(process.cwd(), PIN)}: ${Object.keys(model.owned).length} owned, ${Object.keys(model.referenced).length} referenced tables.`);
}
else {
    const pinned = JSON.parse(fs.readFileSync(PIN, "utf8"));
    const compare = (section) => {
        const a = pinned[section] || {}, b = model[section];
        for (const name of new Set([...Object.keys(a), ...Object.keys(b)])) {
            if (!(name in b)) problems.push(`${section}: ${name} is pinned but no longer in the model`);
            else if (!(name in a)) problems.push(`${section}: ${name} is new: ${JSON.stringify(b[name])}`);
            else if (JSON.stringify(a[name]) !== JSON.stringify(b[name])) problems.push(`${section}: ${name} changed: ${JSON.stringify(a[name])} -> ${JSON.stringify(b[name])}`);
        }
    };
    compare("owned");
    compare("referenced");
    if (JSON.stringify(pinned.sheet_order) !== JSON.stringify(model.sheet_order)) problems.push("the sheet order changed");
}

if (problems.length) {
    console.log(`${problems.length} difference(s) from the pinned SDF schema model:`);
    for (const p of problems) console.log(`  - ${p}`);
    console.log("If the change is intended, review what it does to exports and imports, then run with --update.");
    process.exit(1);
}
console.log(`The SDF schema model matches the pin: ${Object.keys(model.owned).length} owned, ${Object.keys(model.referenced).length} referenced tables.`);
