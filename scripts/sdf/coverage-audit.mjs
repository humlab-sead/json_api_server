/**
 * Global coverage audit (plans/sdf-review-report.md, M16): for every table of
 * site data, the rows that no site reaches. Such rows are outside every SDF
 * export, so SDF can neither show nor edit them, and each export's own
 * self-audit cannot see them. The import refuses to create more (the
 * attachment invariant, §3); this lists the ones there are.
 *
 * Read-only. From the json_api_server directory:
 *   node scripts/sdf/coverage-audit.mjs [--strict]
 * --strict exits non-zero when any orphan exists. Connects with POSTGRES_*, or
 * libpq's PG* variables.
 */
import pkg from "pg";
import SdfSchema from "../../src/Lib/SeadDataFormat/SdfSchema.class.js";
import { quoteIdent } from "../../src/Lib/SeadDataFormat/SdfCommon.js";

const env = process.env;
const pool = new pkg.Pool(env.POSTGRES_HOST ? {
    user: env.POSTGRES_USER, host: env.POSTGRES_HOST, database: env.POSTGRES_DATABASE,
    password: env.POSTGRES_PASS, port: env.POSTGRES_PORT,
} : {});

const client = await pool.connect();
const orphans = [];
try {
    await client.query("begin isolation level repeatable read read only");
    const schema = await SdfSchema.load(client);
    for (const name of schema.fetchOrder) {
        const entry = schema.owned.get(name);
        if (entry.deprecated === "excluded") continue;
        const table = entry.table;
        const res = await client.query(
            `select count(*)::int as n, (array_agg(a.${quoteIdent(table.pk)} order by a.${quoteIdent(table.pk)}))[1:10] as sample
             from public.${quoteIdent(name)} a where not (${schema.reachablePredicate(name, "a")})`);
        if (res.rows[0].n) orphans.push({ table: name, rows: res.rows[0].n, sample: res.rows[0].sample });
    }
    await client.query("rollback");
}
finally {
    client.release();
    await pool.end();
}

if (!orphans.length) {
    console.log("Every row of site data belongs to a site.");
    process.exit(0);
}
console.log("Rows of site data that no site reaches, and that no SDF export therefore contains:");
for (const o of orphans) console.log(`  ${o.table}: ${o.rows} (e.g. ${o.sample.join(", ")}${o.rows > o.sample.length ? ", …" : ""})`);
process.exit(process.argv.includes("--strict") ? 1 : 0);
