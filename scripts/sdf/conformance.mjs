/**
 * SDF export conformance check (plans/sead-data-format-design.html §2, T1).
 *
 * For each site: downloads the rendered workbook and the exporter's structure
 * from a running server, reads the workbook back, and checks that
 *   - every sheet and header is present and verbatim,
 *   - every cell reads back exactly as exported (an empty string reads back
 *     blank, which §8 allows),
 *   - every row's baseline hash recomputes from the cells as read,
 *   - every Text-formatted cell style carries quotePrefix.
 * With --import, the workbook is also uploaded to /sdf/validate, which must
 * accept it with an empty change set: the full empty round trip.
 *
 * Usage, from the json_api_server directory (inside the container, where the
 * POSTGRES_* variables are set):
 *   node scripts/sdf/conformance.mjs --sites 1,79,321
 *   node scripts/sdf/conformance.mjs --all [--import] [--concurrency 2] [--out sdf-conformance.jsonl] [--retry-failed]
 *   node scripts/sdf/conformance.mjs --file workbook.xlsx --site 1
 *     (checks a workbook re-saved elsewhere, e.g. by Excel or LibreOffice,
 *      against a fresh export of the same site)
 *
 * A run over many sites can be interrupted and resumed. Each site's result is
 * appended to the --out file (default sdf-conformance.jsonl with --all) and
 * flushed to disk as soon as it is known; a rerun with the same file skips the
 * sites it already holds, and --retry-failed checks the failed ones again. When
 * the server cannot be reached (down, or restarting after a code change), the
 * site is retried for a while and the run then stops without recording it, so an
 * outage never counts as a failure. The summary covers every site in the file.
 *
 * Options: --base http://localhost:8484 (the server to test).
 * Exits non-zero if any site fails.
 */
import fs from "fs";
import pg from "pg";
import ExcelJS from "exceljs";
import JSZip from "jszip";
import { rowHash } from "../../src/Lib/SeadDataFormat/SdfCommon.js";

const args = parseArgs(process.argv.slice(2));
const base = args.base || `http://localhost:${process.env.API_PORT || 8484}`;
//the import endpoints ask for the server's protected-endpoint password
const env = process.env;
const AUTH = { Authorization: `Basic ${Buffer.from(`${env.PROTECTED_ENDPOINTS_USER ?? env.JAS_PROTECTED_ENDPOINTS_USER ?? "sead"}:${env.PROTECTED_ENDPOINTS_PASS ?? env.JAS_PROTECTED_ENDPOINTS_PASS ?? ""}`).toString("base64")}` };

async function siteList() {
    if (args.sites) return args.sites.split(",").map(Number);
    if (args.site) return [Number(args.site)];
    if (!args.all) {
        console.error("Give --sites 1,2,3, --site N with --file, or --all.");
        process.exit(2);
    }
    const pool = new pg.Pool({
        user: process.env.POSTGRES_USER, host: process.env.POSTGRES_HOST, database: process.env.POSTGRES_DATABASE,
        password: process.env.POSTGRES_PASS, port: process.env.POSTGRES_PORT,
    });
    const ids = (await pool.query("select site_id from public.tbl_sites order by site_id")).rows.map(r => r.site_id);
    await pool.end();
    return ids;
}

async function checkSite(siteId) {
    const started = Date.now();
    const result = { siteId };
    let buffer;
    if (args.file) {
        buffer = fs.readFileSync(args.file);
    }
    else {
        const xr = await fetch(`${base}/sdf/export/${siteId}`);
        if (xr.status !== 200) return { ...result, error: `xlsx ${xr.status}: ${(await xr.text()).slice(0, 300)}` };
        buffer = Buffer.from(await xr.arrayBuffer());
        result.ms = Date.now() - started;
        result.bytes = buffer.length;
    }
    const jr = await fetch(`${base}/sdf/export/${siteId}?format=json`);
    if (jr.status !== 200) return { ...result, error: `json ${jr.status}: ${(await jr.text()).slice(0, 300)}` };
    const bundle = await jr.json();

    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(buffer);

    let cells = 0;
    const problems = [];
    const note = p => { if (problems.length < 10) problems.push(p); };
    let hashFailures = 0;

    for (const sheet of bundle.sheets) {
        const ws = wb.getWorksheet(sheet.name);
        if (!ws) { note(`missing sheet ${sheet.name}`); continue; }
        const header = sheet.columns.map((c, i) => ws.getRow(1).getCell(i + 1).value);
        if (header.join("\t") !== sheet.columns.map(c => c.key).join("\t")) note(`header differs on ${sheet.name}`);
        const dataIndexes = sheet.columns.map((c, i) => (c.kind === "data" ? i : -1)).filter(i => i >= 0);

        sheet.rows.forEach((expected, r) => {
            const row = ws.getRow(r + 2);
            const got = sheet.columns.map((c, i) => readCell(row.getCell(i + 1)));
            expected.forEach((value, i) => {
                cells++;
                const want = value === "" ? null : value;
                if (!sameValue(want, got[i])) {
                    note(`${sheet.name}!${row.getCell(i + 1).address} ${sheet.columns[i].key}: ${JSON.stringify(want)} -> ${JSON.stringify(got[i])}`);
                }
            });
            if (rowHash(dataIndexes.map(i => got[i])) !== sheet.baseline[r].hash) hashFailures++;
        });
    }

    const zip = await JSZip.loadAsync(buffer);
    const styles = await zip.file("xl/styles.xml").async("string");
    const textStyles = styles.match(/<xf\b[^>]*numFmtId="49"[^>]*>/g) || [];
    const missingQuotePrefix = textStyles.filter(x => !/quotePrefix="1"/.test(x)).length;
    if (!args.file && missingQuotePrefix) note(`${missingQuotePrefix} Text cell styles without quotePrefix`);

    if (args.import) {
        const vr = await fetch(`${base}/sdf/validate`, { method: "POST", headers: AUTH, body: buffer });
        const report = await vr.json();
        result.import = report.summary || null;
        if (!report.ok) note(`import refused: ${JSON.stringify(report.errors.slice(0, 3))}`);
        else if (!report.summary.empty) note(`import not empty: ${JSON.stringify(report.summary)} ${JSON.stringify(report.change_set).slice(0, 300)}`);
        if (report.warnings && report.warnings.length) note(`import warnings: ${JSON.stringify(report.warnings.slice(0, 3))}`);
    }

    return {
        ...result, sheets: bundle.sheets.length, cells, hashFailures,
        ok: problems.length === 0 && hashFailures === 0, problems,
    };
}

/**
 * A re-saved workbook may turn CRLF into LF; §8 compares line endings after
 * normalising, so the cell check does too. The hash check needs no allowance:
 * the canonical form already normalises them.
 */
function sameValue(want, got) {
    if (typeof want === "string" && typeof got === "string") {
        return want === got || (args.file && want.replace(/\r\n/g, "\n") === got.replace(/\r\n/g, "\n"));
    }
    return want === got;
}

function readCell(cell) {
    let v = cell.value;
    if (v === null || v === undefined) return null;
    if (typeof v === "object" && v.richText) v = v.richText.map(t => t.text).join("");
    if (typeof v === "object" && "formula" in v) return { formula: v.formula };
    return v;
}

function parseArgs(argv) {
    const out = {};
    for (let i = 0; i < argv.length; i++) {
        const key = argv[i].replace(/^--/, "");
        const next = argv[i + 1];
        if (next === undefined || next.startsWith("--")) out[key] = true;
        else { out[key] = next; i++; }
    }
    return out;
}

/** The server could not be reached at all: not a result for the site. */
function unreachable(err) {
    const code = err && (err.cause?.code || err.code);
    return err instanceof TypeError && /fetch failed/.test(err.message)
        || ["ECONNREFUSED", "ECONNRESET", "EPIPE", "UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT"].includes(code);
}

/** Checks a site, waiting out a server that is briefly unreachable. Returns null if it stays so. */
async function checkWithRetry(siteId) {
    const waits = [2, 5, 10, 20, 30, 60]; //seconds
    for (let attempt = 0; ; attempt++) {
        try { return await checkSite(siteId); }
        catch (err) {
            if (!unreachable(err)) return { siteId, error: String(err && err.stack || err).slice(0, 400) };
            if (attempt >= waits.length) return null;
            console.log(`site ${siteId}: server unreachable (${err.cause?.code || err.message}); retrying in ${waits[attempt]} s`);
            await new Promise(r => setTimeout(r, waits[attempt] * 1000));
        }
    }
}

/** The results already in a progress file, latest per site; a torn last line is ignored. */
function readProgress(path) {
    const results = new Map();
    if (!path || !fs.existsSync(path)) return results;
    for (const line of fs.readFileSync(path, "utf8").split("\n")) {
        if (!line.trim()) continue;
        try {
            const r = JSON.parse(line);
            if (r && r.siteId !== undefined) results.set(r.siteId, r);
        }
        catch {
            //a line cut short by a crash: that site is checked again
        }
    }
    return results;
}

const passed = r => !r.error && r.ok;
const sites = await siteList();
const concurrency = Math.max(1, parseInt(args.concurrency) || 1);
const outPath = args.out || (args.all ? "sdf-conformance.jsonl" : null);
const build = await fetch(`${base}/sdf/version`).then(r => r.json()).then(v => v.exporter_build).catch(() => null);

const previous = readProgress(outPath);
const todo = sites.filter(id => !previous.has(id) || (args["retry-failed"] && !passed(previous.get(id))));
if (previous.size) {
    const earlierBuilds = new Set([...previous.values()].map(r => r.build).filter(Boolean));
    console.log(`Resuming from ${outPath}: ${previous.size} site(s) already checked` +
        ` (${[...previous.values()].filter(r => !passed(r)).length} failed); ${todo.length} to go.`);
    if (build && [...earlierBuilds].some(b => b !== build)) {
        console.log(`Note: earlier results came from ${[...earlierBuilds].join(", ")}; this server is ${build}.`);
    }
}

//one line per site, appended and flushed at once, so an interruption loses at most the sites in flight
const fd = outPath ? fs.openSync(outPath, "a") : null;
//a line torn by a crash has no newline: end it, so the next record starts on a line of its own
if (fd !== null && fs.fstatSync(fd).size > 0) {
    const last = Buffer.alloc(1);
    const rfd = fs.openSync(outPath, "r");
    fs.readSync(rfd, last, 0, 1, fs.fstatSync(rfd).size - 1);
    fs.closeSync(rfd);
    if (last[0] !== 0x0a) fs.writeSync(fd, "\n");
}
const record = result => {
    if (fd === null) return;
    fs.writeSync(fd, JSON.stringify({ ...result, build, checked_at: new Date().toISOString() }) + "\n");
    fs.fsyncSync(fd);
};

let failed = 0, done = 0, stopped = false;
const queue = [...todo];
await Promise.all(Array.from({ length: concurrency }, async () => {
    while (queue.length && !stopped) {
        const siteId = queue.shift();
        const result = await checkWithRetry(siteId);
        if (result === null) {
            stopped = true;
            console.log(`The server at ${base} stayed unreachable; stopping. Site ${siteId} and later ones are not recorded; run again to continue.`);
            break;
        }
        if (!passed(result)) failed++;
        done++;
        record(result);
        if (!fd || !passed(result) || todo.length <= 20) console.log(JSON.stringify(result));
        else if (done % 100 === 0) console.log(`${done}/${todo.length} checked in this run, ${failed} failed`);
    }
}));
if (fd !== null) fs.closeSync(fd);

const all = fd !== null ? readProgress(outPath) : null;
const total = all ? sites.filter(id => all.has(id)) : todo.slice(0, done);
const totalFailed = all ? total.filter(id => !passed(all.get(id))).length : failed;
console.log(`${done} site(s) checked in this run, ${failed} failed.` +
    (all ? ` In ${outPath}: ${total.length} of ${sites.length} checked, ${totalFailed} failed${total.length < sites.length ? ", the rest still to go" : ""}.` : ""));
process.exit(stopped ? 3 : totalFailed ? 1 : 0);
