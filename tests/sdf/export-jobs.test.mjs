/**
 * SDF export jobs (SeadDataFormat: /sdf/export/jobs) and the export limits: a job
 * reports its progress, hands its file over once, can be cancelled, and reports
 * a refusal; an export takes any number of sites unless SDF_MAX_SITES is set, and
 * Excel's row limit is the one limit it cannot be configured past.
 * The exporter and renderer are stand-ins, so no database is needed.
 * Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import SeadDataFormat from "../../src/EndpointModules/SeadDataFormat.class.js";
import SdfExporter from "../../src/Lib/SeadDataFormat/SdfExporter.class.js";
import { EXCEL_MAX_ROW } from "../../src/Lib/SeadDataFormat/SdfCommon.js";

const BUNDLE = { meta: [["exported_at", "2026-11-23T12:00:00Z"]], sheets: [] };

/** The module with its routes captured, and an exporter and renderer that wait on `gate`. */
function sdf({ exportError = null } = {}) {
    const routes = {};
    const route = method => (path, ...handlers) => { routes[`${method} ${path}`] = handlers[handlers.length - 1]; };
    const app = {
        expressApp: { get: route("GET"), post: route("POST"), delete: route("DELETE") },
        authHandler: { requireRoleOrBasicAuth: () => (req, res, next) => next() },
    };
    const module = new SeadDataFormat(app);
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    module.exporter = {
        export: async (ids, opts) => {
            opts.onProgress(1, 2);
            await gate;
            if (exportError) throw exportError;
            opts.onProgress(2, 2);
            return BUNDLE;
        },
    };
    module.renderer = {
        render: async (bundle, opts) => {
            opts.onProgress(5, 10);
            opts.onProgress(10, 10);
            return Buffer.from("xlsx");
        },
    };
    return { module, routes, release };
}

/** Calls a route handler with a fake request; resolves to { status, body, headers }. */
function call(routes, key, { params = {}, body = {} } = {}) {
    return new Promise(resolve => {
        const out = { status: 200, headers: {} };
        const res = {
            status(code) { out.status = code; return res; },
            setHeader(name, value) { out.headers[name] = value; },
            json(value) { out.body = value; resolve(out); },
            send(value) { out.body = value; resolve(out); },
            end() { resolve(out); },
        };
        routes[key]({ params, body, query: {}, headers: {} }, res);
    });
}

const tick = () => new Promise(resolve => setImmediate(resolve));

async function waitFor(routes, id, states) {
    for (let i = 0; i < 100; i++) {
        const { body } = await call(routes, "GET /sdf/export/jobs/:id", { params: { id } });
        if (states.includes(body.state)) return body;
        await tick();
    }
    throw new Error(`job never reached ${states}`);
}

test("a job reports progress, then hands over its file once", async () => {
    const { routes, release } = sdf();
    const started = await call(routes, "POST /sdf/export/jobs", { body: { siteIds: Array.from({ length: 500 }, (_, i) => i + 1) } });
    assert.equal(started.status, 202);
    const id = started.body.id;

    const running = await waitFor(routes, id, ["running"]);
    assert.equal(running.phase, "gathering");
    assert.equal(running.siteCount, 500);
    assert.ok(running.progress > 0 && running.progress < 0.2, `progress ${running.progress}`);

    release();
    const done = await waitFor(routes, id, ["done"]);
    assert.equal(done.progress, 1);
    assert.equal(done.filename, "sead_sites_500_20261123.xlsx");

    const file = await call(routes, "GET /sdf/export/jobs/:id/file", { params: { id } });
    assert.equal(file.status, 200);
    assert.equal(String(file.body), "xlsx");
    assert.match(file.headers["Content-Disposition"], /sead_sites_500_20261123\.xlsx/);

    const again = await call(routes, "GET /sdf/export/jobs/:id/file", { params: { id } });
    assert.equal(again.status, 404);
});

test("the file is refused until the job is done", async () => {
    const { routes, release } = sdf();
    const { body: { id } } = await call(routes, "POST /sdf/export/jobs", { body: { siteIds: [1] } });
    await waitFor(routes, id, ["running"]);
    const early = await call(routes, "GET /sdf/export/jobs/:id/file", { params: { id } });
    assert.equal(early.status, 409);
    release();
    await waitFor(routes, id, ["done"]);
});

test("a cancelled job stops and is gone", async () => {
    const { routes, release } = sdf();
    const { body: { id } } = await call(routes, "POST /sdf/export/jobs", { body: { siteIds: [1, 2] } });
    await waitFor(routes, id, ["running"]);
    const cancel = await call(routes, "DELETE /sdf/export/jobs/:id", { params: { id } });
    assert.equal(cancel.status, 204);
    release();
    for (let i = 0; i < 20; i++) await tick();
    const after = await call(routes, "GET /sdf/export/jobs/:id", { params: { id } });
    assert.equal(after.status, 404);
});

test("a refused export is reported with the exporter's message", async () => {
    const err = Object.assign(new Error("Too many rows."), { code: "export_too_large", statusCode: 413, detail: { rows: 2 } });
    const { routes, release } = sdf({ exportError: err });
    const { body: { id } } = await call(routes, "POST /sdf/export/jobs", { body: { siteIds: [1] } });
    release();
    const failed = await waitFor(routes, id, ["failed"]);
    assert.deepEqual(failed.error, { error: "Too many rows.", code: "export_too_large", detail: { rows: 2 } });
});

test("any number of sites is accepted unless SDF_MAX_SITES is set", async () => {
    const { module } = sdf();
    assert.equal(module.maxSites, null);
    const many = Array.from({ length: 5000 }, (_, i) => i + 1);
    assert.equal(module.sanitizeSiteIds(many).valid, true);
    module.maxSites = 100;
    assert.equal(module.sanitizeSiteIds(many).valid, false);
});

test("an export with a sheet past Excel's row limit is refused", () => {
    const exporter = new SdfExporter({});
    const sheet = rows => ({ name: "t", rows: new Array(rows) });
    const machine = rows => ({ baseline: new Array(rows + 1), columns: [[]] });
    //the baseline holds one row per data row of every sheet, so it overflows first
    exporter._requireExcelRows([sheet(600000), sheet(400000)], machine(1000000), [1, 2]);
    assert.throws(
        () => exporter._requireExcelRows([sheet(600000), sheet(500000)], machine(1100000), [1, 2]),
        err => err.code === "export_too_large" && err.detail.sheet === "_sdf_baseline" && err.detail.maxRows === EXCEL_MAX_ROW);
});
