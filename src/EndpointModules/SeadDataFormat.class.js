import crypto from "crypto";
import busboy from "busboy";
import SdfExporter from "../Lib/SeadDataFormat/SdfExporter.class.js";
import SdfValidator from "../Lib/SeadDataFormat/SdfValidator.class.js";
import SdfChangeRequest from "../Lib/SeadDataFormat/SdfChangeRequest.class.js";
import SdfRenderer from "../Lib/SeadDataFormat/SdfRenderer.class.js";
import { guideConfig } from "../Lib/SeadDataFormat/SdfGuide.js";
import { SDF_VERSION } from "../Lib/SeadDataFormat/SdfCommon.js";
import { attributionOf } from "../Lib/Auth/AuthIdentity.js";

/**
 * HTTP surface for the SEAD Data Format (plans/sead-data-format-design.html).
 *
 *   GET  /sdf/version                        format version and what is implemented
 *   GET  /sdf/export/:siteId                 one site, as .xlsx
 *   POST /sdf/export   { siteIds: [..] }     several sites in one workbook, as .xlsx
 *   POST /sdf/export/jobs { siteIds: [..] }  the same as a job, answered at once (202) with its id;
 *   GET  /sdf/export/jobs/:id                its state and progress, for a progress bar
 *   GET  /sdf/export/jobs/:id/file           the finished .xlsx, once; the job is then gone
 *   DELETE /sdf/export/jobs/:id              cancels it, or discards an unfetched file
 *   POST /sdf/validate                       an edited workbook (multipart field "file", or the
 *                                            raw .xlsx as the body): import stages 1-4, a JSON report
 *   POST /sdf/change-request                 the same upload: stages 1-5, a .zip change-request bundle
 *                                            for sead_change_control, or the report (422) if it does
 *                                            not validate
 *
 * Either export takes ?format=json to return the exporter's tabular structure
 * instead of the rendered file. That form is for tests and debugging; clients
 * download the .xlsx, which the server renders itself (spec D17).
 *
 * Nothing here writes to any database. An import ends in a Sqitch change
 * request for sead_change_control (spec §10); SDF never applies changes itself.
 *
 * The import endpoints are for SEAD's data managers: a signed-in user with the
 * sysadmin role (src/Lib/Auth/UserRoles.js), which is how the client's "Import
 * data" dialog reaches them, or the server's basic auth for protected endpoints
 * (PROTECTED_ENDPOINTS_USER / _PASS), which is how scripts do. SDF jobs hold
 * whole workbooks in memory on the server that also
 * serves the public browser, so at most SDF_MAX_JOBS run at once, a few more
 * wait, and the rest are turned away with 503.
 *
 * An export takes any number of sites unless SDF_MAX_SITES is set. Many sites
 * take minutes (about 40 s for 500 sites), which is what the export jobs are for:
 * the client polls the job for a progress bar instead of holding one request
 * open. A finished file waits EXPORT_JOB_TTL_MS for its download.
 */

const XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
const DEFAULT_MAX_UPLOAD_MB = 64;
const DEFAULT_MAX_JOBS = 2;
const MAX_WAITING_JOBS = 8;
//How long a finished export job keeps its file for the download
const EXPORT_JOB_TTL_MS = 10 * 60 * 1000;
//The share of an export job's progress bar given to reading the database; the
//rest is writing the workbook, which takes about four times as long
const GATHER_SHARE = 0.2;
//Who may use the import endpoints, besides the protected-endpoint password
const IMPORT_ROLE = "sysadmin";

class SeadDataFormat {
    constructor(app) {
        this.app = app;
        this.exporter = new SdfExporter(app);
        this.validator = new SdfValidator(app);
        this.changeRequest = new SdfChangeRequest(app);
        this.maxUploadBytes = (parseInt(process.env.SDF_MAX_UPLOAD_MB) || DEFAULT_MAX_UPLOAD_MB) * 1024 * 1024;
        this.renderer = new SdfRenderer(guideConfig());
        this.maxSites = parseInt(process.env.SDF_MAX_SITES) || null;
        this.maxJobs = parseInt(process.env.SDF_MAX_JOBS) || DEFAULT_MAX_JOBS;
        this.running = 0;
        this.waiting = [];
        this.exportJobs = new Map();
        this.setupEndpoints();
    }

    /** True when no slot is free and no more may wait for one. */
    _busy() {
        return this.running >= this.maxJobs && this.waiting.length >= MAX_WAITING_JOBS;
    }

    _sendBusy(res) {
        res.setHeader("Retry-After", "30");
        res.status(503).json({ error: "The server is busy with other SDF exports or imports. Try again in a minute.", code: "busy" });
    }

    /** Runs fn once a slot is free. The caller has checked _busy(). */
    async _inSlot(fn) {
        if (this.running >= this.maxJobs) {
            await new Promise(resolve => this.waiting.push(resolve));
        }
        this.running++;
        try {
            return await fn();
        }
        finally {
            this.running--;
            const next = this.waiting.shift();
            if (next) next();
        }
    }

    /** Runs an SDF job once a slot is free; refuses when too many are waiting. */
    async _job(res, fn) {
        if (this._busy()) {
            this._sendBusy(res);
            return undefined;
        }
        return await this._inSlot(fn);
    }

    sanitizeSiteIds(raw) {
        if (!Array.isArray(raw)) {
            return { valid: false, error: "'siteIds' must be an array of positive integers." };
        }
        const ids = raw.map(v => Number(v)).filter(v => Number.isInteger(v) && v > 0);
        if (ids.length !== raw.length) {
            return { valid: false, error: "Every id in 'siteIds' must be a positive integer." };
        }
        if (ids.length === 0) {
            return { valid: false, error: "'siteIds' is empty." };
        }
        if (this.maxSites && ids.length > this.maxSites) {
            return { valid: false, error: `At most ${this.maxSites} sites can be exported in one workbook.` };
        }
        return { valid: true, ids: [...new Set(ids)].sort((a, b) => a - b) };
    }

    setupEndpoints() {
        const app = this.app.expressApp;
        const importer = this.app.authHandler.requireRoleOrBasicAuth(IMPORT_ROLE);

        app.get("/sdf/version", (req, res) => {
            res.status(200).json({
                sdf_version: SDF_VERSION,
                exporter_build: `${this.app.appName}-${this.app.appVersion}`,
                export: "xlsx",
                import: ["validate", "change-request"],
                max_sites: this.maxSites,
            });
        });

        app.get("/sdf/export/:siteId", async (req, res) => {
            const siteId = Number(req.params.siteId);
            if (!Number.isInteger(siteId) || siteId <= 0) {
                return res.status(400).json({ error: "siteId must be a positive integer." });
            }
            await this._job(res, () => this._runExport(req, res, [siteId]));
        });

        app.post("/sdf/export", async (req, res) => {
            const { valid, ids, error } = this.sanitizeSiteIds(req.body && req.body.siteIds);
            if (!valid) {
                return res.status(400).json({ error: `Bad input - ${error}` });
            }
            await this._job(res, () => this._runExport(req, res, ids));
        });

        app.post("/sdf/export/jobs", (req, res) => {
            const { valid, ids, error } = this.sanitizeSiteIds(req.body && req.body.siteIds);
            if (!valid) {
                return res.status(400).json({ error: `Bad input - ${error}` });
            }
            if (this._busy()) {
                return this._sendBusy(res);
            }
            const job = this._startExportJob(ids, this._exportedBy(req));
            res.status(202).json(this._jobStatus(job));
        });

        app.get("/sdf/export/jobs/:id", (req, res) => {
            const job = this.exportJobs.get(req.params.id);
            if (!job) {
                return res.status(404).json({ error: "No such export job. It may have expired.", code: "job_not_found" });
            }
            res.status(200).json(this._jobStatus(job));
        });

        app.get("/sdf/export/jobs/:id/file", (req, res) => {
            const job = this.exportJobs.get(req.params.id);
            if (!job) {
                return res.status(404).json({ error: "No such export job. It may have expired.", code: "job_not_found" });
            }
            if (job.state !== "done") {
                return res.status(409).json({ error: `The export is ${job.state}, not done.`, code: "job_not_done" });
            }
            const buffer = job.file;
            this._forgetJob(job);
            res.status(200);
            res.setHeader("Content-Type", XLSX_MIME);
            res.setHeader("Content-Disposition", `attachment; filename="${job.filename}"`);
            res.setHeader("Content-Length", buffer.length);
            res.setHeader("Access-Control-Expose-Headers", "Content-Disposition, Content-Length");
            res.send(buffer);
        });

        app.delete("/sdf/export/jobs/:id", (req, res) => {
            const job = this.exportJobs.get(req.params.id);
            if (job) {
                job.cancelled = true;
                if (job.state !== "queued" && job.state !== "running") this._forgetJob(job);
            }
            res.status(204).end();
        });

        app.post("/sdf/validate", importer, async (req, res) => {
            try {
                const buffer = await this._readUpload(req);
                await this._job(res, async () => {
                    const { report } = await this.validator.validate(buffer);
                    res.status(report.ok ? 200 : 422).json(report);
                });
            }
            catch (err) {
                this._sendError(res, err, "validating the workbook");
            }
        });

        app.post("/sdf/change-request", importer, async (req, res) => {
            try {
                const buffer = await this._readUpload(req);
                const result = await this._job(res, () => this.changeRequest.generate(buffer, { author: this._exportedBy(req) }));
                if (!result) return; //turned away as busy
                const { report, bundle, name } = result;
                if (!report.ok) {
                    return res.status(422).json(report);
                }
                console.log(`SDF change request ${name}: ${JSON.stringify(report.summary)}`);
                res.status(200);
                res.setHeader("Content-Type", "application/zip");
                res.setHeader("Content-Disposition", `attachment; filename="${name}.zip"`);
                res.setHeader("Content-Length", bundle.length);
                res.setHeader("Access-Control-Expose-Headers", "Content-Disposition, Content-Length");
                res.send(bundle);
            }
            catch (err) {
                this._sendError(res, err, "generating the change request");
            }
        });
    }

    /**
     * The uploaded workbook, from a multipart form (field "file") or as the raw
     * request body. Refused above SDF_MAX_UPLOAD_MB.
     */
    _readUpload(req) {
        const limit = this.maxUploadBytes;
        const tooLarge = () => Object.assign(new Error(`The file is larger than the ${limit / 1024 / 1024} MB allowed.`), { code: "upload_too_large", statusCode: 413 });
        const contentType = req.headers["content-type"] || "";
        return new Promise((resolve, reject) => {
            if (!contentType.startsWith("multipart/form-data")) {
                const chunks = [];
                let size = 0;
                req.on("data", chunk => {
                    size += chunk.length;
                    if (size > limit) { reject(tooLarge()); req.destroy(); return; }
                    chunks.push(chunk);
                });
                req.on("end", () => resolve(Buffer.concat(chunks)));
                req.on("error", reject);
                return;
            }
            let found = null;
            const bb = busboy({ headers: req.headers, limits: { files: 1, fileSize: limit } });
            bb.on("file", (name, stream) => {
                const chunks = [];
                stream.on("data", chunk => chunks.push(chunk));
                stream.on("limit", () => reject(tooLarge()));
                stream.on("end", () => { if (name === "file" || !found) found = Buffer.concat(chunks); });
            });
            bb.on("close", () => found
                ? resolve(found)
                : reject(Object.assign(new Error("No file was uploaded (expected a form field named \"file\")."), { code: "no_file", statusCode: 400 })));
            bb.on("error", reject);
            req.pipe(bb);
        });
    }

    _sendError(res, err, doing) {
        if (err && err.code && err.statusCode) {
            if (err.statusCode >= 500) console.error(`SDF error while ${doing}:`, err.code, err.message, JSON.stringify(err.detail));
            return res.status(err.statusCode).json({ error: err.message, code: err.code, detail: err.detail });
        }
        console.error(`Unhandled error while ${doing}:`, err);
        res.status(500).json({ error: `Internal server error while ${doing}.` });
    }

    //------------------------------------------------------------ export jobs

    /** Creates an export job and runs it in the background. The caller has checked _busy(). */
    _startExportJob(siteIds, exportedBy) {
        const job = {
            id: crypto.randomUUID(),
            state: "queued", //queued, running, done, failed, cancelled
            phase: "queued", //queued, gathering, writing, done
            progress: 0,
            siteCount: siteIds.length,
            started: Date.now(),
            cancelled: false,
            file: null,
            filename: null,
            error: null,
            expiry: null,
        };
        this.exportJobs.set(job.id, job);

        const checkCancelled = () => {
            if (job.cancelled) throw Object.assign(new Error("The export was cancelled."), { code: "cancelled" });
        };

        this._inSlot(async () => {
            checkCancelled();
            job.state = "running";
            job.phase = "gathering";
            const bundle = await this.exporter.export(siteIds, {
                exportedBy,
                onProgress: (done, total) => {
                    checkCancelled();
                    job.progress = GATHER_SHARE * done / total;
                },
            });
            checkCancelled();
            job.phase = "writing";
            const buffer = await this.renderer.render(bundle, {
                onProgress: (done, total) => {
                    checkCancelled();
                    job.progress = GATHER_SHARE + (1 - GATHER_SHARE) * done / total;
                },
            });
            job.file = buffer;
            job.filename = this._filename(siteIds, bundle);
            job.state = "done";
            job.phase = "done";
            job.progress = 1;
            this._logExport(siteIds, bundle, buffer, job.started, "job");
        })
            .catch(err => {
                if (job.cancelled || (err && err.code === "cancelled")) {
                    job.state = "cancelled";
                    return;
                }
                job.state = "failed";
                if (err && err.code && err.statusCode) {
                    if (err.statusCode >= 500) console.error("SDF export job refused:", err.code, err.message, JSON.stringify(err.detail));
                    job.error = { error: err.message, code: err.code, detail: err.detail };
                }
                else {
                    console.error("Unhandled error in SDF export job:", err);
                    job.error = { error: "Internal server error while building the SDF workbook." };
                }
            })
            .finally(() => {
                if (job.cancelled) {
                    this._forgetJob(job);
                    return;
                }
                //kept for its download (or its error), then dropped
                job.expiry = setTimeout(() => this._forgetJob(job), EXPORT_JOB_TTL_MS);
                job.expiry.unref();
            });

        return job;
    }

    _forgetJob(job) {
        if (job.expiry) clearTimeout(job.expiry);
        job.file = null;
        this.exportJobs.delete(job.id);
    }

    _jobStatus(job) {
        const status = {
            id: job.id,
            state: job.state,
            phase: job.phase,
            progress: Math.round(job.progress * 1000) / 1000,
            siteCount: job.siteCount,
            elapsedMs: Date.now() - job.started,
        };
        if (job.state === "queued") status.position = this.waiting.length;
        if (job.state === "done") {
            status.filename = job.filename;
            status.bytes = job.file ? job.file.length : null;
        }
        if (job.error) status.error = job.error;
        return status;
    }

    _filename(siteIds, bundle) {
        const date = new Map(bundle.meta).get("exported_at").slice(0, 10).replace(/-/g, "");
        return siteIds.length <= 5
            ? `sead_site_${siteIds.join("_")}_${date}.xlsx`
            : `sead_sites_${siteIds.length}_${date}.xlsx`;
    }

    _logExport(siteIds, bundle, buffer, started, how = "request") {
        console.log(`SDF export (${how}) of ${siteIds.length} site(s) [${siteIds.slice(0, 10).join(",")}${siteIds.length > 10 ? ",…" : ""}]: ` +
            `${bundle.sheets.length} sheets, ${buffer.length} bytes, ${Date.now() - started} ms`);
    }

    async _runExport(req, res, siteIds) {
        const started = Date.now();
        try {
            const bundle = await this.exporter.export(siteIds, { exportedBy: this._exportedBy(req) });

            if (req.query.format === "json") {
                return res.status(200).json(bundle);
            }

            const buffer = await this.renderer.render(bundle);
            const name = this._filename(siteIds, bundle);
            this._logExport(siteIds, bundle, buffer, started);

            res.status(200);
            res.setHeader("Content-Type", XLSX_MIME);
            res.setHeader("Content-Disposition", `attachment; filename="${name}"`);
            res.setHeader("Content-Length", buffer.length);
            res.setHeader("Access-Control-Expose-Headers", "Content-Disposition, Content-Length");
            res.send(buffer);
        }
        catch (err) {
            if (err && err.code && err.statusCode) {
                if (err.statusCode >= 500) console.error("SDF export refused:", err.code, err.message, JSON.stringify(err.detail));
                return res.status(err.statusCode).json({ error: err.message, code: err.code, detail: err.detail });
            }
            console.error("Unhandled error in SDF export:", err);
            res.status(500).json({ error: "Internal server error while building the SDF workbook." });
        }
    }

    _exportedBy(req) {
        const handler = this.app.authHandler;
        const user = handler && typeof handler.getUser === "function" && typeof req.isAuthenticated === "function"
            ? handler.getUser(req) : null;
        return attributionOf(user);
    }
}

export default SeadDataFormat;
