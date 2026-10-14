import zlib from 'zlib';

/*
* Class: NeotomaSites
* Serves the sites of the Neotoma Paleoecology Database (https://www.neotomadb.org/) to the result map's
* "External data" layer.
*
* Neotoma's API can't be called from the browser - its CORS policy turns SEAD's origins away - and listing
* its sites is slow: a single request of about two minutes and 20+ MB. Its offset paging can't split that
* up either, since the offset doesn't count sites, and pages come back short and overlapping. So the sites
* are harvested in the background into the neotoma_sites collection and served from there. A harvest runs
* at startup when the collection is missing or older than REFRESH_INTERVAL_DAYS, and the age is checked
* again once a day.
*/
class NeotomaSites {
    static SITES_URL = "https://api.neotomadb.org/v2.0/data/sites?limit=1000000";
    static COLLECTION = "neotoma_sites";
    static REFRESH_INTERVAL_DAYS = 7;
    static REFRESH_CHECK_INTERVAL_MS = 24 * 60 * 60 * 1000;
    //The listing takes about two minutes when Neotoma is well
    static HARVEST_TIMEOUT_MS = 10 * 60 * 1000;
    //A harvest with fewer sites than this share of the stored ones is taken to be a failed listing rather
    //than Neotoma having lost its sites, and the stored ones are kept
    static MIN_HARVEST_RATIO = 0.5;

    constructor(app) {
        this.app = app;
        this.harvestPromise = null;
        //The site list only changes with a harvest, so it is serialized and compressed once rather than per request
        this.siteListPromise = null;
        this.setupEndpoints();

        this.app.mongoReady.then(() => {
            this.refreshIfStale();
            setInterval(() => this.refreshIfStale(), NeotomaSites.REFRESH_CHECK_INTERVAL_MS);
        });
    }

    setupEndpoints() {
        /*
        * GET /neotoma/sites
        * Every harvested site as just its id, name and position - enough to plot it. There are some 32k of
        * them, so the list goes out gzipped when the client accepts it, as the router doesn't compress.
        */
        this.app.expressApp.get('/neotoma/sites', async (req, res) => {
            try {
                const siteList = await this.getSiteList();
                res.header("Content-type", "application/json");
                if(!siteList) {
                    res.status(503);
                    res.send(JSON.stringify({
                        error: "The Neotoma sites have not been harvested yet",
                        harvesting: this.harvestPromise != null
                    }, null, 2));
                    return;
                }

                res.header("Vary", "Accept-Encoding");
                if(req.acceptsEncodings("gzip")) {
                    res.header("Content-Encoding", "gzip");
                    res.send(siteList.gzip);
                }
                else {
                    res.send(siteList.json);
                }
            }
            catch(error) {
                console.error("NeotomaSites error:", error);
                res.status(500).send(JSON.stringify({ error: "Internal server error" }, null, 2));
            }
        });

        /*
        * GET /neotoma/site/:siteid
        * One site with its collection units and their datasets, for the map popup.
        */
        this.app.expressApp.get('/neotoma/site/:siteid', async (req, res) => {
            const siteId = Number.parseInt(req.params.siteid, 10);
            if(!Number.isInteger(siteId)) {
                res.status(400).send(JSON.stringify({ error: "Invalid site id" }, null, 2));
                return;
            }

            try {
                const site = await this.app.mongo.collection(NeotomaSites.COLLECTION)
                    .findOne({ siteid: siteId }, { projection: { _id: 0 } });
                if(!site) {
                    res.status(404).send(JSON.stringify({ error: "Site not found" }, null, 2));
                    return;
                }

                res.header("Content-type", "application/json");
                res.send(JSON.stringify(site, null, 2));
            }
            catch(error) {
                console.error("NeotomaSite error:", error);
                res.status(500).send(JSON.stringify({ error: "Internal server error" }, null, 2));
            }
        });
    }

    getSiteList() {
        if(!this.siteListPromise) {
            this.siteListPromise = this.buildSiteList().then((siteList) => {
                //Nothing harvested yet is not cached, so the list appears as soon as a harvest lands
                if(!siteList) {
                    this.siteListPromise = null;
                }
                return siteList;
            }, (error) => {
                this.siteListPromise = null;
                throw error;
            });
        }
        return this.siteListPromise;
    }

    async buildSiteList() {
        const collection = this.app.mongo.collection(NeotomaSites.COLLECTION);
        const sites = await collection
            .find({}, { projection: { _id: 0, siteid: 1, sitename: 1, longitude: 1, latitude: 1 } })
            .sort({ siteid: 1 })
            .toArray();
        if(sites.length === 0) {
            return null;
        }

        const newest = await collection.findOne({}, { projection: { _id: 0, harvested_at: 1 } });
        //Not indented: whitespace would be a third of a list this long
        const json = JSON.stringify({
            harvested_at: newest.harvested_at,
            total: sites.length,
            sites: sites
        });
        return { json: json, gzip: zlib.gzipSync(json) };
    }

    async refreshIfStale() {
        try {
            const newest = await this.app.mongo.collection(NeotomaSites.COLLECTION)
                .findOne({}, { projection: { _id: 0, harvested_at: 1 } });
            const age = newest ? Date.now() - newest.harvested_at.getTime() : Infinity;
            if(age < NeotomaSites.REFRESH_INTERVAL_DAYS * 24 * 60 * 60 * 1000) {
                return;
            }
            await this.harvest();
        }
        catch(error) {
            console.error("Neotoma sites harvest failed:", error);
        }
    }

    harvest() {
        if(!this.harvestPromise) {
            this.harvestPromise = this.runHarvest().finally(() => {
                this.harvestPromise = null;
            });
        }
        return this.harvestPromise;
    }

    /*
    * Function: runHarvest
    * Replaces the stored sites with a fresh listing. The sites are written to a collection of their own and
    * renamed over the old one, so the endpoints never see half a harvest.
    */
    async runHarvest() {
        console.log("Harvesting Neotoma sites from "+NeotomaSites.SITES_URL);
        const response = await fetch(NeotomaSites.SITES_URL, { signal: AbortSignal.timeout(NeotomaSites.HARVEST_TIMEOUT_MS) });
        if(!response.ok) {
            throw new Error(`Neotoma answered ${response.status} ${response.statusText}`);
        }
        const body = await response.json();
        const sites = NeotomaSites.normalizeSites(body.data, new Date());

        const db = this.app.mongo;
        const storedCount = await db.collection(NeotomaSites.COLLECTION).estimatedDocumentCount();
        if(sites.length === 0 || sites.length < storedCount * NeotomaSites.MIN_HARVEST_RATIO) {
            throw new Error(`Neotoma listed ${sites.length} sites where ${storedCount} are stored, keeping the stored ones`);
        }

        const harvestCollectionName = NeotomaSites.COLLECTION+"_harvest";
        if(await db.listCollections({ name: harvestCollectionName }).hasNext()) {
            await db.collection(harvestCollectionName).drop();
        }
        const harvestCollection = db.collection(harvestCollectionName);
        await harvestCollection.insertMany(sites);
        await harvestCollection.createIndex({ siteid: 1 }, { unique: true });
        await harvestCollection.rename(NeotomaSites.COLLECTION, { dropTarget: true });

        this.siteListPromise = null;
        console.log(`Harvested ${sites.length} Neotoma sites`);
    }

    /*
    * Function: normalizeSites
    * Turns Neotoma's site listing into one document per site with a position to plot. Sites without a
    * geography can't be plotted and are left out. The listing repeats a collection unit once per dataset
    * it holds, so the units are merged here, each with all its datasets.
    */
    static normalizeSites(rows, harvestedAt) {
        const sites = new Map();
        for(const row of rows || []) {
            if(row.siteid == null) {
                continue;
            }
            const position = NeotomaSites.representativePoint(row.geography);
            if(!position) {
                continue;
            }

            let site = sites.get(row.siteid);
            if(!site) {
                site = {
                    siteid: row.siteid,
                    sitename: row.sitename,
                    sitedescription: row.sitedescription,
                    altitude: row.altitude,
                    longitude: position.longitude,
                    latitude: position.latitude,
                    geometry_type: position.geometryType,
                    collectionunits: [],
                    harvested_at: harvestedAt
                };
                sites.set(row.siteid, site);
            }
            NeotomaSites.addCollectionUnits(site, row.collectionunits);
        }
        return [...sites.values()];
    }

    static addCollectionUnits(site, collectionUnits) {
        for(const row of collectionUnits || []) {
            if(row.collectionunitid == null) {
                continue;
            }

            let unit = site.collectionunits.find(u => u.collectionunitid === row.collectionunitid);
            if(!unit) {
                unit = {
                    collectionunitid: row.collectionunitid,
                    collectionunit: row.collectionunit,
                    handle: row.handle,
                    collectionunittype: row.collectionunittype,
                    datasets: []
                };
                site.collectionunits.push(unit);
            }

            for(const dataset of row.datasets || []) {
                if(dataset.datasetid == null || unit.datasets.some(d => d.datasetid === dataset.datasetid)) {
                    continue;
                }
                unit.datasets.push({ datasetid: dataset.datasetid, datasettype: dataset.datasettype });
            }
        }
    }

    /*
    * Function: representativePoint
    * The point a site is plotted at. Most Neotoma sites are points; the rest are polygons - lake outlines,
    * mostly - and a few lines, which are plotted at the middle of their bounding box. That can fall outside
    * a crescent-shaped lake, but not by enough to matter for a marker.
    */
    static representativePoint(geography) {
        let geometry = geography;
        if(typeof geography === "string") {
            try {
                geometry = JSON.parse(geography);
            }
            catch(error) {
                return null;
            }
        }
        if(!geometry || !Array.isArray(geometry.coordinates)) {
            return null;
        }

        const bounds = { minLon: Infinity, maxLon: -Infinity, minLat: Infinity, maxLat: -Infinity };
        const extend = (coordinates) => {
            if(!Array.isArray(coordinates)) {
                return;
            }
            if(Number.isFinite(coordinates[0]) && Number.isFinite(coordinates[1])) {
                bounds.minLon = Math.min(bounds.minLon, coordinates[0]);
                bounds.maxLon = Math.max(bounds.maxLon, coordinates[0]);
                bounds.minLat = Math.min(bounds.minLat, coordinates[1]);
                bounds.maxLat = Math.max(bounds.maxLat, coordinates[1]);
            }
            else {
                coordinates.forEach(extend);
            }
        };
        extend(geometry.coordinates);
        if(!Number.isFinite(bounds.minLon)) {
            return null;
        }

        const longitude = (bounds.minLon + bounds.maxLon) / 2;
        const latitude = (bounds.minLat + bounds.maxLat) / 2;
        if(Math.abs(longitude) > 180 || Math.abs(latitude) > 90) {
            return null;
        }
        return { longitude: longitude, latitude: latitude, geometryType: geometry.type };
    }
}

export default NeotomaSites;
