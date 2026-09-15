import SdfExporter from "../Lib/SeadDataFormat/SdfExporter.class.js";
import SdfImporter from "../Lib/SeadDataFormat/SdfImporter.class.js";
import { SDF_VERSION } from "../Lib/SeadDataFormat/SdfCommon.js";

/**
 * HTTP surface for the SEAD Data Format (plans/sead-data-format-design.html).
 *
 *   GET  /sdf/version                    format version and capabilities
 *   GET  /sdf/export/:siteId             one-site export (convenience)
 *   POST /sdf/export   { siteIds:[], profile:"readable"|"complete" }
 *   POST /sdf/import   <an SDF bundle>   validate + diff; writes nothing
 *
 * The export returns the bundle as JSON: sheets of ordered columns and rows,
 * plus a manifest. Rendering it to .xlsx is left to the client, which already
 * does that well with ExcelJS.
 */
class SeadDataFormat {
    constructor(app) {
        this.app = app;
        this.exporter = new SdfExporter(app);
        this.importer = new SdfImporter(app);
        this.setupEndpoints();
    }

    sanitizeSiteIds(raw) {
        if (!Array.isArray(raw)) {
            return { valid: false, error: "'siteIds' must be an array of positive integers." };
        }
        const ids = raw.map(v => parseInt(v)).filter(v => Number.isInteger(v) && v > 0);
        if (ids.length !== raw.length) {
            return { valid: false, error: "Every id in 'siteIds' must be a positive integer." };
        }
        if (ids.length === 0) {
            return { valid: false, error: "'siteIds' is empty." };
        }
        return { valid: true, ids };
    }

    setupEndpoints() {
        const app = this.app.expressApp;

        app.get("/sdf/version", (req, res) => {
            res.status(200).json({
                sdf_version: SDF_VERSION,
                exporter_build: `${this.app.appName}-${this.app.appVersion}`,
                profiles: ["readable", "complete"],
                import: "validate-and-diff only — writes nothing to the database (D8)",
                //0: the export audits its own completeness (manifest.coverage.analysis_entities)
                //1: server-side flattening; 3: the reader, validating only.
                //2 is partial: the typed analysis subtables are joined and the
                //owned-table gap is closed, but .xlsx rendering is client-side
                //and the _Raw appendix (D6) is not built.
                phases_implemented: [0, 1, 3],
                phases_partial: [2],
            });
        });

        app.get("/sdf/export/:siteId", async (req, res) => {
            const siteId = parseInt(req.params.siteId);
            if (!Number.isInteger(siteId) || siteId <= 0) {
                return res.status(400).json({ error: "siteId must be a positive integer." });
            }
            await this._runExport(res, [siteId], req.query.profile);
        });

        app.post("/sdf/export", async (req, res) => {
            const { valid, ids, error } = this.sanitizeSiteIds(req.body && req.body.siteIds);
            if (!valid) {
                return res.status(400).json({ error: `Bad input - ${error}` });
            }
            await this._runExport(res, ids, req.body.profile);
        });

        app.post("/sdf/import", async (req, res) => {
            try {
                const report = await this.importer.validate(req.body);
                res.status(report.ok ? 200 : 422).json(report);
            } catch (err) {
                console.error("Unhandled error in POST /sdf/import:", err);
                res.status(500).json({ error: "Internal server error while validating the SDF bundle." });
            }
        });
    }

    async _runExport(res, siteIds, profile) {
        try {
            const bundle = await this.exporter.export(siteIds, { profile });
            res.status(200);
            res.setHeader("Content-Type", "application/json");
            res.setHeader("Content-Disposition", `attachment; filename="sdf_${siteIds.join("_")}.json"`);
            res.send(JSON.stringify(bundle, null, 2));
        } catch (err) {
            if (err && err.statusCode) {
                return res.status(err.statusCode).json({ error: err.message });
            }
            console.error("Unhandled error in SDF export:", err);
            res.status(500).json({ error: "Internal server error while building the SDF bundle." });
        }
    }
}

export default SeadDataFormat;
