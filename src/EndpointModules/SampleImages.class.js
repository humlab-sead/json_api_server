import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/*
* Class: SampleImages
* Serves the images of physical samples - e.g. photomicrographs of thin sections - from tbl_sample_images,
* together with the sample, sample group and site they belong to, and the image metadata of
* tbl_sample_image_metadata where there is any.
*
* The image files live in SAMPLE_IMAGES_DIR, as full/<image_location> and thumb/<image_location>, where
* image_location is what tbl_sample_images holds, e.g. "athena/bjornhovda_ts1.jpg".
*
* tbl_sample_image_metadata only exists in databases with the thin section data, so the metadata is left out
* (null) where the table is absent.
*/
class SampleImages {
    static DEFAULT_IMAGES_DIR = path.resolve(__dirname, '../../assets/images/sample_images');
    //Image files don't change under their name, but a day is short enough for a replaced image to show soon
    static CACHE_MAX_AGE = "1d";
    static NATURAL_COLLATOR = new Intl.Collator("sv", { numeric: true, sensitivity: "base" });

    constructor(app) {
        this.app = app;
        this.imagesDir = path.resolve(process.env.SAMPLE_IMAGES_DIR || SampleImages.DEFAULT_IMAGES_DIR);
        this.hasMetadataTable = false;
        this.setupEndpoints();
    }

    setupEndpoints() {
        /*
        * Endpoint: POST /sample-images/sites
        * Body: { site_ids: [1, 2, ...] }
        * An array of the images of the samples at those sites, ordered by site name, then sample name.
        */
        this.app.expressApp.post('/sample-images/sites', async (req, res) => {
            const siteIds = req.body ? req.body.site_ids : null;
            if(typeof(siteIds) == "undefined" || siteIds === null) {
                this.sendJson(res, []);
                return;
            }
            if(!this.app.isValidSiteIdsInput(siteIds)) {
                res.status(400);
                res.send("Bad input - site_ids should be an array of site IDs\n");
                return;
            }

            try {
                this.sendJson(res, await this.fetchSampleImages(siteIds.map(Number)));
            }
            catch(error) {
                console.error("SampleImages error:", error);
                res.status(500).send(JSON.stringify({ error: "Internal server error" }));
            }
        });

        /*
        * Endpoint: GET /sample-images/site/:siteId
        * The same array for a single site.
        */
        this.app.expressApp.get('/sample-images/site/:siteId', async (req, res) => {
            const siteId = Number(req.params.siteId);
            if(!Number.isInteger(siteId) || siteId < 1) {
                res.status(400);
                res.send("Bad input - the site ID should be an integer\n");
                return;
            }

            try {
                this.sendJson(res, await this.fetchSampleImages([siteId]));
            }
            catch(error) {
                console.error("SampleImages error:", error);
                res.status(500).send(JSON.stringify({ error: "Internal server error" }));
            }
        });

        /*
        * Endpoints: GET /sample-images/full/<image_location> and GET /sample-images/thumb/<image_location>
        */
        ["full", "thumb"].forEach(variant => {
            this.app.expressApp.get('/sample-images/'+variant+'/*', (req, res) => {
                this.sendImageFile(req, res, path.join(this.imagesDir, variant));
            });
        });
    }

    sendJson(res, data) {
        res.header("Content-type", "application/json");
        res.send(JSON.stringify(data));
    }

    sendImageFile(req, res, baseDir) {
        const requestedPath = req.params[0];
        if(!requestedPath || requestedPath.includes("\0")) {
            return res.status(400).send('Invalid path');
        }

        const resolvedPath = path.resolve(baseDir, requestedPath);

        //Prevent path traversal outside of the images directory
        if(!resolvedPath.startsWith(baseDir + path.sep)) {
            return res.status(400).send('Invalid path');
        }

        if(!fs.existsSync(resolvedPath) || !fs.statSync(resolvedPath).isFile()) {
            return res.status(404).send('Image not found');
        }

        res.sendFile(resolvedPath, { maxAge: SampleImages.CACHE_MAX_AGE, dotfiles: "deny" });
    }

    /*
    * Whether tbl_sample_image_metadata exists. Only a yes is remembered, so that the metadata shows up
    * without a restart when the table is created after the server has started.
    */
    async checkMetadataTable(pgClient) {
        if(!this.hasMetadataTable) {
            const result = await pgClient.query("SELECT to_regclass('public.tbl_sample_image_metadata') AS metadata_table");
            this.hasMetadataTable = result.rows[0].metadata_table !== null;
        }
        return this.hasMetadataTable;
    }

    async fetchSampleImages(siteIds) {
        if(siteIds.length == 0) {
            return [];
        }

        const pgClient = await this.app.getDbConnection();
        try {
            const withMetadata = await this.checkMetadataTable(pgClient);

            const metadataColumns = withMetadata ? `,
                    md.sample_image_metadata_id,
                    md.magnification,
                    md.field_of_view_mm::float8 AS field_of_view_mm,
                    md.polarisation,
                    md.camera_specs,
                    md.resin_type,
                    md.original_file_name` : "";
            const metadataJoin = withMetadata ? "LEFT JOIN tbl_sample_image_metadata md ON md.sample_image_id = si.sample_image_id" : "";

            const result = await pgClient.query(`
                SELECT
                    si.sample_image_id,
                    si.image_name,
                    it.image_type,
                    si.description,
                    si.image_location,
                    s.site_id,
                    s.site_name,
                    sg.sample_group_id,
                    sg.sample_group_name,
                    ps.physical_sample_id,
                    ps.sample_name${metadataColumns}
                FROM tbl_sample_images si
                JOIN tbl_image_types it ON it.image_type_id = si.image_type_id
                JOIN tbl_physical_samples ps ON ps.physical_sample_id = si.physical_sample_id
                JOIN tbl_sample_groups sg ON sg.sample_group_id = ps.sample_group_id
                JOIN tbl_sites s ON s.site_id = sg.site_id
                ${metadataJoin}
                WHERE s.site_id = ANY($1::int[])
            `, [siteIds]);

            const collator = SampleImages.NATURAL_COLLATOR;
            return result.rows
                .sort((a, b) => collator.compare(a.site_name || "", b.site_name || "")
                    || collator.compare(a.sample_name || "", b.sample_name || "")
                    || a.sample_image_id - b.sample_image_id)
                .map(row => this.buildImageObject(row, withMetadata));
        }
        finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    buildImageObject(row, withMetadata) {
        return {
            sample_image_id: row.sample_image_id,
            image_name: row.image_name,
            image_type: row.image_type,
            description: row.description,
            image_location: row.image_location,
            full_url: "sample-images/full/"+row.image_location,
            thumb_url: "sample-images/thumb/"+row.image_location,
            site_id: row.site_id,
            site_name: row.site_name,
            sample_group_id: row.sample_group_id,
            sample_group_name: row.sample_group_name,
            physical_sample_id: row.physical_sample_id,
            sample_name: row.sample_name,
            metadata: withMetadata && row.sample_image_metadata_id !== null ? {
                magnification: row.magnification,
                field_of_view_mm: row.field_of_view_mm,
                polarisation: row.polarisation,
                camera_specs: row.camera_specs,
                resin_type: row.resin_type,
                original_file_name: row.original_file_name
            } : null
        };
    }
}

export default SampleImages;
