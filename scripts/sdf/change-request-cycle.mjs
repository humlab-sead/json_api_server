/**
 * SDF change requests, end to end against a real database (plans/sdf-review-report.md,
 * test plan items 3 and 5): export a site, edit the workbook the way a curator would,
 * generate the change request, add it to sead_change_control with
 * bin/add-sdf-change-request, deploy it with Sqitch, check the database, revert it,
 * and check that a change request gone stale aborts without changing anything.
 *
 * This writes to the database, so it only runs against the throwaway copy that
 * scratch-db.sh provides, never against a real one. From the json_api_server
 * directory:
 *   scripts/sdf/scratch-db.sh run node scripts/sdf/change-request-cycle.mjs [--site 1] [--show]
 *
 * --show prints the generated deploy script and what Sqitch and the helper said.
 *
 * The SDF modules are used directly, without a running server. Exports and
 * validation connect as json_api_server's read-only role (POSTGRES_*); Sqitch and
 * the simulated concurrent edit run inside the scratch container as the owner.
 * Exits non-zero if any check fails.
 */
import { execFileSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";
import pkg from "pg";
import ExcelJS from "exceljs";
import JSZip from "jszip";
import SdfExporter from "../../src/Lib/SeadDataFormat/SdfExporter.class.js";
import SdfRenderer from "../../src/Lib/SeadDataFormat/SdfRenderer.class.js";
import SdfValidator from "../../src/Lib/SeadDataFormat/SdfValidator.class.js";
import SdfChangeRequest from "../../src/Lib/SeadDataFormat/SdfChangeRequest.class.js";
import { guideConfig } from "../../src/Lib/SeadDataFormat/SdfGuide.js";

const { Pool } = pkg;
const args = Object.fromEntries(process.argv.slice(2).reduce((acc, a, i, all) =>
    (a.startsWith("--") ? [...acc, [a.slice(2), all[i + 1]]] : acc), []));
const SITE = Number(args.site || 1);
const SHOW = process.argv.includes("--show");
const show = (title, text) => { if (SHOW) console.log(`\n----- ${title}\n${text.trimEnd()}\n-----`); };
const CONTAINER = process.env.SCRATCH_CONTAINER;
if (!CONTAINER || process.env.POSTGRES_PORT === "5432") {
    console.error("Run this through scripts/sdf/scratch-db.sh run, against the throwaway database only.");
    process.exit(2);
}
const TARGET = `db:pg://${process.env.PGUSER}@127.0.0.1/${process.env.PGDATABASE}`;

const app = {
    appName: "sead-json-api-server",
    appVersion: "sdf-test",
    pgPool: new Pool({
        user: process.env.POSTGRES_USER, host: process.env.POSTGRES_HOST, database: process.env.POSTGRES_DATABASE,
        password: process.env.POSTGRES_PASS, port: process.env.POSTGRES_PORT, max: 4,
    }),
};
const exporter = new SdfExporter(app);
const renderer = new SdfRenderer(guideConfig({}));
const validator = new SdfValidator(app);
const generator = new SdfChangeRequest(app);

let failures = 0;
const check = (name, condition, detail) => {
    console.log(`${condition ? "  ok  " : "  FAIL"} ${name}${!condition && detail !== undefined ? `\n         ${JSON.stringify(detail).slice(0, 800)}` : ""}`);
    if (!condition) failures++;
};

/** Runs a command in the scratch container; returns { ok, out }. */
function inContainer(command, { user = "postgres" } = {}) {
    try {
        const out = execFileSync("podman", ["exec", "-u", user, "-w", "/sead_change_control", CONTAINER, "bash", "-c", command],
            { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
        return { ok: true, out };
    }
    catch (err) {
        return { ok: false, out: `${err.stdout || ""}${err.stderr || ""}` };
    }
}

const sqitch = cmd => inContainer(`sqitch ${cmd} --target ${TARGET} -C sdf`);
const psql = sql => inContainer(`psql -XAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -U ${process.env.PGUSER} -d ${process.env.PGDATABASE} -c "${sql.replace(/"/g, '\\"')}"`);

/** Data and system cells of every exported sheet, keyed by table and id. */
function cells(bundle) {
    const out = new Map();
    for (const sheet of bundle.sheets) {
        const pk = sheet.columns.findIndex(c => c.isPk);
        sheet.columns.forEach((c, i) => {
            if (c.kind !== "data" && c.kind !== "system") return;
            for (const row of sheet.rows) out.set(`${sheet.table}|${row[pk]}|${c.key}`, row[i]);
        });
    }
    return out;
}

function differences(before, after) {
    const diffs = [];
    for (const key of new Set([...before.keys(), ...after.keys()])) {
        const a = before.has(key) ? before.get(key) : "<absent>";
        const b = after.has(key) ? after.get(key) : "<absent>";
        if (a !== b) diffs.push({ key, before: a, after: b });
    }
    return diffs;
}

function col(ws, key) {
    const row = ws.getRow(1);
    for (let c = 1; c <= ws.columnCount; c++) if (row.getCell(c).value === key) return c;
    throw new Error(`no column ${key} on ${ws.name}`);
}

const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "sdf-cycle-"));
try {
    //---------------------------------------------------------------- setup
    console.log(`Setup: export site ${SITE} and make a curator's edits`);
    if (!inContainer("command -v jq").ok) {
        const installed = inContainer("apt-get update -qq && apt-get install -y -qq jq > /dev/null", { user: "root" });
        if (!installed.ok) throw new Error(`could not install jq in the scratch container: ${installed.out}`);
    }

    const original = await exporter.export([SITE]);
    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(await renderer.render(original));

    const ps = wb.getWorksheet("physical_samples");
    const sg = wb.getWorksheet("sample_groups");
    const sites = wb.getWorksheet("sites");
    const types = wb.getWorksheet("sample_types");
    const idOf = (ws, r, key) => ws.getRow(r).getCell(col(ws, key)).value;

    const edited = {
        sampleName: { id: idOf(ps, 2, "physical_sample_id"), value: "0123 SDF test\nsecond line" },
        sampleType: { id: idOf(ps, 3, "physical_sample_id"), from: idOf(ps, 3, "sample_type_id") },
        groupName: { id: idOf(sg, 2, "sample_group_id"), value: "SDF test group" },
        latitude: { id: idOf(sites, 2, "site_id"), value: 56.8663888 },
    };
    //another existing sample type, picked by id
    for (let r = 2; r <= types.rowCount; r++) {
        const id = idOf(types, r, "sample_type_id");
        if (id !== edited.sampleType.from) { edited.sampleType.value = id; break; }
    }
    ps.getRow(2).getCell(col(ps, "sample_name")).value = edited.sampleName.value;
    ps.getRow(3).getCell(col(ps, "sample_type_id")).value = edited.sampleType.value;
    sg.getRow(2).getCell(col(sg, "sample_group_name")).value = edited.groupName.value;
    sites.getRow(2).getCell(col(sites, "latitude_dd")).value = edited.latitude.value;
    //a proposed sample type, and a sample pointed at it: reported, never applied
    const tRow = types.rowCount + 1;
    types.getRow(tRow).getCell(col(types, "sample_type_id")).value = "NEW-sdftest";
    types.getRow(tRow).getCell(col(types, "type_name")).value = "SDF test type";
    edited.blocked = { id: idOf(ps, 4, "physical_sample_id") };
    ps.getRow(4).getCell(col(ps, "sample_type_id")).value = "NEW-sdftest";

    const workbook = Buffer.from(await wb.xlsx.writeBuffer());

    //------------------------------------------------------------- generate
    console.log("Generate the change request");
    const { report, bundle, name } = await generator.generate(workbook, { author: "SDF cycle test" });
    check("validates", report.ok, report.errors);
    check("four updates", report.summary?.updates === 4, report.change_set?.updates?.map(u => [u.table, u.id, u.fields.map(f => f.column)]));
    check("no inserts, deletes or conflicts", report.summary?.inserts === 0 && report.summary?.deletes === 0 && report.summary?.conflicts === 0, report.summary);
    check("the sample pointed at the proposed type is blocked, not applied",
        report.change_set?.blocked?.some(b => b.id === edited.blocked.id) && !report.change_set?.updates?.some(u => u.id === edited.blocked.id),
        report.change_set?.blocked);
    check("the change is named after the export", /^\d{8}_DML_SDF_SITE_\d+_[0-9A-F]{8}$/.test(name || ""), name);

    const zip = await JSZip.loadAsync(bundle);
    const changeJson = JSON.parse(await zip.file(`${name}/change.json`).async("string"));
    const deploySql = await zip.file(`${name}/deploy/${name}.sql`).async("string");
    show("deploy script", deploySql);
    check("change.json names the sdf project", changeJson.project === "sdf", changeJson.project);
    check("no followup.xlsx", !Object.keys(zip.files).some(f => /followup/.test(f)), Object.keys(zip.files));
    check("every update sets date_updated", (deploySql.match(/^update .*"date_updated" = '/gm) || []).length === 4, deploySql.slice(0, 2000));
    check("the deploy script verifies itself before commit", /-- 6\. Verify[\s\S]*SDF verify:[\s\S]*\ncommit;/.test(deploySql));

    //------------------------------------------------- add to change control
    console.log("Add it to sead_change_control with bin/add-sdf-change-request");
    const zipPath = path.join(workDir, `${name}.zip`);
    fs.writeFileSync(zipPath, bundle);
    execFileSync("podman", ["cp", zipPath, `${CONTAINER}:/tmp/${name}.zip`]);
    inContainer(`chmod 644 /tmp/${name}.zip`, { user: "root" });
    inContainer("chown -R postgres /sead_change_control", { user: "root" });
    const added = inContainer(`bin/add-sdf-change-request /tmp/${name}.zip --no-issues`);
    show("add-sdf-change-request", added.out);
    check("the helper adds the change request", added.ok, added.out);
    const plan = inContainer("cat sdf/sqitch.plan").out;
    check("the plan lists it with the export id in its note", plan.includes(name) && plan.includes(`sdf-export:${original.meta.find(m => m[0] === "export_id")[1]}`), plan);
    const placed = inContainer(`cat sdf/deploy/${name}.sql`).out;
    check("the bundle's deploy script is in place, with a Deploy line", placed.startsWith(`-- Deploy sdf: ${name}`) && placed.includes("SDF guard:"), placed.slice(0, 300));
    const again = inContainer(`bin/add-sdf-change-request /tmp/${name}.zip --no-issues`);
    check("adding the same bundle twice is refused", !again.ok && /already in sdf\/sqitch.plan/.test(again.out), again.out);

    //the edited rows exactly as the database holds them, scale and microseconds included
    const rowText = () => psql(
        `select t::text from public.tbl_physical_samples t where physical_sample_id in (${edited.sampleName.id}, ${edited.sampleType.id})
         union all select t::text from public.tbl_sample_groups t where sample_group_id = ${edited.groupName.id}
         union all select t::text from public.tbl_sites t where site_id = ${edited.latitude.id} order by 1`).out;
    const textBefore = rowText();

    //----------------------------------------------------------------- deploy
    console.log("Deploy it, as deploy-staging does (--no-verify), then verify");
    const deployed = sqitch("deploy --no-verify");
    show("sqitch deploy", deployed.out);
    check("deploys", deployed.ok, deployed.out);
    const verified = sqitch("verify");
    check("Sqitch verify passes", verified.ok, verified.out);

    const afterDeploy = cells(await exporter.export([SITE]));
    const diffs = differences(cells(original), afterDeploy);
    const expected = new Map([
        [`tbl_physical_samples|${edited.sampleName.id}|sample_name`, edited.sampleName.value],
        [`tbl_physical_samples|${edited.sampleType.id}|sample_type_id`, edited.sampleType.value],
        [`tbl_sample_groups|${edited.groupName.id}|sample_group_name`, edited.groupName.value],
        [`tbl_sites|${edited.latitude.id}|latitude_dd`, edited.latitude.value],
    ]);
    const unexpected = diffs.filter(d => !expected.has(d.key) && !d.key.endsWith("|date_updated"));
    check("a fresh export shows exactly the edits", [...expected].every(([k, v]) => afterDeploy.get(k) === v) && unexpected.length === 0,
        { unexpected, missing: [...expected].filter(([k, v]) => afterDeploy.get(k) !== v) });
    const stamped = diffs.filter(d => d.key.endsWith("|date_updated")).map(d => d.key.split("|").slice(0, 2).join("|"));
    check("date_updated changed on the four edited rows and nowhere else",
        stamped.length === 4 && [...expected.keys()].every(k => stamped.includes(k.split("|").slice(0, 2).join("|"))), stamped);
    check("date_updated is the same literal on every edited row", new Set(diffs.filter(d => d.key.endsWith("|date_updated")).map(d => d.after)).size === 1);

    const resubmit = await validator.validate(workbook);
    check("the same workbook cannot be submitted again", resubmit.report.errors.some(e => e.code === "already_submitted"), resubmit.report.errors);

    //----------------------------------------------------------------- revert
    console.log("Revert it");
    const reverted = sqitch("revert -y");
    check("reverts", reverted.ok, reverted.out);
    const afterRevert = differences(cells(original), cells(await exporter.export([SITE])));
    check("the database is exactly as before, date_updated included", afterRevert.length === 0, afterRevert.slice(0, 10));
    check("the edited rows are byte for byte as before", rowText() === textBefore, { before: textBefore, after: rowText() });

    //----------------------------------------------------- a stale change request
    console.log("A row changes after generation: the deploy must abort and change nothing");
    check("redeploys after the revert", sqitch("deploy --no-verify").ok);
    check("and reverts again", sqitch("revert -y").ok);
    const concurrent = psql(`update public.tbl_sample_groups set sample_group_name = sample_group_name || ' (concurrent)' where sample_group_id = ${edited.groupName.id}`);
    check("a concurrent edit is made", concurrent.ok, concurrent.out);
    const stale = sqitch("deploy --no-verify");
    show("sqitch deploy (stale)", stale.out);
    check("the deploy aborts on its guard", !stale.ok && /SDF guard: public\.tbl_sample_groups/.test(stale.out), stale.out);
    const status = sqitch("status");
    show("sqitch status", status.out);
    check("the change is not recorded as deployed", !status.out.includes(`# ${name}`) || /No changes deployed|Undeployed change/.test(status.out), status.out);
    const untouched = differences(cells(original), cells(await exporter.export([SITE])));
    check("nothing but the concurrent edit changed", untouched.length === 1 && untouched[0].key === `tbl_sample_groups|${edited.groupName.id}|sample_group_name`, untouched);
}
catch (err) {
    console.error(err);
    failures++;
}
finally {
    fs.rmSync(workDir, { recursive: true, force: true });
    await app.pgPool.end();
}

console.log(failures ? `${failures} check(s) failed.` : "All checks passed.");
process.exit(failures ? 1 : 0);
