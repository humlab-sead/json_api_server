/*
* Class: Gadm
* Serves administrative boundaries from the gadm schema as polygons the map filter can use.
*
* The client's map filter (`sites_polygon`) takes rings of [latitude, longitude] pairs and
* asks the query API for the sites inside them. GADM geometries are far too detailed for
* that - Sweden alone is 3338 rings and 14k points - so nothing here hands out raw
* geometry. Each boundary is reduced to the few largest rings, each simplified until it
* fits a point budget, which is what makes a country or a municipality expressible as a
* handful of polygons in a URL.
*
* Holes and inner rings are dropped: the filter has no concept of them, and a lake inside
* a region does not change which sites the region contains in any way that matters here.
*/
class Gadm {
    //Levels as GADM names them: 0 country, 1 first-level region, 2 second-level (municipality
    //or county, depending on the country).
    static LEVELS = [0, 1, 2];
    //Enough rings for a mainland plus its main islands, without turning one filter into a
    //hundred OR:ed ST_Within expressions
    static DEFAULT_MAX_RINGS = 4;
    static MAX_RINGS_LIMIT = 12;
    //Points per ring. Measured against full-resolution containment: at 150 points Skåne's
    //outline misses 1 of its 301 sites, at 250 it misses none, and the query cost is
    //unchanged (12 KB of SQL, ~0.1 s) - so the budget is set where accuracy stops improving
    //rather than as low as the filter can bear. Every point still costs two picks, which is
    //what keeps this bounded at all.
    static DEFAULT_MAX_POINTS = 250;
    static MAX_POINTS_LIMIT = 500;
    //Rings smaller than this share of the largest one are skirries and sandbanks - they cost
    //a polygon each and contain nothing.
    static MIN_RING_AREA_RATIO = 0.001;
    //Simplification tolerances in degrees, tried smallest first: the finest one whose result
    //fits the point budget wins. Ratios of roughly 1.5 keep the overshoot small.
    static TOLERANCE_LADDER = [0.001, 0.0015, 0.0025, 0.004, 0.006, 0.01, 0.015, 0.025, 0.04,
                               0.06, 0.1, 0.15, 0.25, 0.4, 0.6, 1.0];

    constructor(app) {
        this.app = app;
        this.setupEndpoints();
    }

    setupEndpoints() {
        /*
        * GET /gadm/areas?q=<name>[&level=0|1|2][&country=<name>][&limit=<n>]
        * Finds administrative areas by name. Names repeat across the world - "York" is eight
        * different places - so every hit carries its parents, and the caller is expected to
        * choose rather than assume the first row.
        */
        this.app.expressApp.get('/gadm/areas', async (req, res) => {
            const query = (req.query.q || "").trim();
            if(query.length < 2) {
                res.status(400);
                res.send(JSON.stringify({ error: "Query must be at least 2 characters long" }, null, 2));
                return;
            }

            const levels = this.resolveLevels(req.query.level);
            if(levels === false) {
                res.status(400);
                res.send(JSON.stringify({ error: "Invalid level. Must be 0 (country), 1 (region) or 2 (municipality)" }, null, 2));
                return;
            }

            const limit = this.clampInteger(req.query.limit, 20, 1, 100);
            const country = (req.query.country || "").trim();

            try {
                const areas = await this.searchAreas(query, levels, country, limit);
                res.header("Content-type", "application/json");
                res.send(JSON.stringify({ query: query, areas: areas }, null, 2));
            }
            catch(error) {
                console.error("Gadm area search error:", error);
                res.status(500);
                res.send(JSON.stringify({ error: "Internal server error" }, null, 2));
            }
        });

        /*
        * GET /gadm/area/:gid/polygons[&maxRings=<n>][&maxPoints=<n>]
        * The boundary of one area, as rings of [latitude, longitude] pairs - the coordinate
        * order the map filter and the query API both use.
        */
        this.app.expressApp.get('/gadm/area/:gid/polygons', async (req, res) => {
            const gid = (req.params.gid || "").trim();
            if(!this.isValidGid(gid)) {
                res.status(400);
                res.send(JSON.stringify({ error: "Invalid gid" }, null, 2));
                return;
            }

            const maxRings = this.clampInteger(req.query.maxRings, Gadm.DEFAULT_MAX_RINGS, 1, Gadm.MAX_RINGS_LIMIT);
            const maxPoints = this.clampInteger(req.query.maxPoints, Gadm.DEFAULT_MAX_POINTS, 20, Gadm.MAX_POINTS_LIMIT);

            try {
                const area = await this.getAreaPolygons(gid, maxRings, maxPoints);
                if(!area) {
                    res.status(404);
                    res.send(JSON.stringify({ error: "No area with gid "+gid }, null, 2));
                    return;
                }
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(area, null, 2));
            }
            catch(error) {
                console.error("Gadm polygon error:", error);
                res.status(500);
                res.send(JSON.stringify({ error: "Internal server error" }, null, 2));
            }
        });
    }

    /*
    * Function: searchAreas
    * Searches one union of the three level tables. Exact matches come first, then matches
    * that begin with the term, then the rest - so "Sweden" doesn't arrive behind "Swedes"
    * of some other level. Area is the final tiebreaker, since the larger of two identically
    * named places is the more likely one to be meant.
    */
    async searchAreas(query, levels, country, limit) {
        const pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            throw new Error("No database connection");
        }

        try {
            const sql = `
                WITH areas AS (
                    SELECT 0 AS level, gid, name_0 AS name, NULL::text AS region, name_0 AS country,
                           NULL::text AS area_type, ST_Area(geom) AS planar_area
                    FROM gadm.adm_0
                    UNION ALL
                    SELECT 1, gid, name_1, NULL::text, name_0, type, ST_Area(geom)
                    FROM gadm.adm_1
                    UNION ALL
                    SELECT 2, gid, name_2, name_1, name_0, type, ST_Area(geom)
                    FROM gadm.adm_2
                )
                SELECT level, gid, name, region, country, area_type
                FROM areas
                WHERE level = ANY($2::int[])
                  AND name ILIKE '%' || $1 || '%'
                  AND ($3 = '' OR country ILIKE $3 || '%')
                ORDER BY
                    CASE WHEN lower(name) = lower($1) THEN 0
                         WHEN name ILIKE $1 || '%' THEN 1
                         ELSE 2 END,
                    level,
                    planar_area DESC
                LIMIT $4
            `;
            const result = await pgClient.query(sql, [query, levels, country, limit]);
            return result.rows.map(row => ({
                gid: row.gid,
                level: row.level,
                name: row.name,
                region: row.region,
                country: row.country,
                type: row.area_type,
            }));
        }
        finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    /*
    * Function: getAreaPolygons
    * Reduces one area's geometry to filter-sized rings.
    *
    * The work is done in the database rather than here because the alternative is shipping
    * hundreds of megabytes of geometry to Node to throw nearly all of it away: adm_2 alone
    * is 268 MB. Rings are ranked by planar area (cheap, and only ever compared within one
    * area), the largest few kept, and each simplified with the finest tolerance that fits
    * the point budget.
    */
    async getAreaPolygons(gid, maxRings, maxPoints) {
        const pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            throw new Error("No database connection");
        }

        try {
            const sql = `
                WITH area AS (
                    SELECT 0 AS level, gid, name_0 AS name, NULL::text AS region, name_0 AS country, geom
                    FROM gadm.adm_0 WHERE gid = $1
                    UNION ALL
                    SELECT 1, gid, name_1, NULL::text, name_0, geom FROM gadm.adm_1 WHERE gid = $1
                    UNION ALL
                    SELECT 2, gid, name_2, name_1, name_0, geom FROM gadm.adm_2 WHERE gid = $1
                ),
                rings AS (
                    SELECT (ST_Dump(geom)).geom AS ring FROM area
                ),
                ranked AS (
                    SELECT ST_ExteriorRing(ring) AS outline,
                           ST_Area(ring) AS planar_area,
                           row_number() OVER (ORDER BY ST_Area(ring) DESC) AS ring_number
                    FROM rings
                ),
                kept AS (
                    SELECT outline, ring_number
                    FROM ranked
                    WHERE ring_number <= $2
                      AND planar_area >= (SELECT max(planar_area) FROM ranked) * $5
                ),
                /* The finest tolerance whose result fits the budget. Falls back to the
                   coarsest one in the ladder for a ring that fits at none of them. */
                fitted AS (
                    SELECT ring_number,
                           COALESCE((
                               SELECT ST_SimplifyPreserveTopology(outline, tolerance)
                               FROM unnest($4::float8[]) AS tolerance
                               WHERE ST_NPoints(ST_SimplifyPreserveTopology(outline, tolerance)) <= $3
                               ORDER BY tolerance
                               LIMIT 1
                           ), ST_SimplifyPreserveTopology(outline, $6)) AS outline
                    FROM kept
                )
                SELECT (SELECT level FROM area) AS level,
                       (SELECT name FROM area) AS name,
                       (SELECT region FROM area) AS region,
                       (SELECT country FROM area) AS country,
                       (SELECT count(*) FROM rings) AS total_rings,
                       fitted.ring_number,
                       ST_NPoints(fitted.outline) AS points,
                       ST_AsGeoJSON(fitted.outline, 5)::json -> 'coordinates' AS coordinates
                FROM fitted
                ORDER BY fitted.ring_number
            `;
            const result = await pgClient.query(sql, [
                gid, maxRings, maxPoints, Gadm.TOLERANCE_LADDER, Gadm.MIN_RING_AREA_RATIO,
                Gadm.TOLERANCE_LADDER[Gadm.TOLERANCE_LADDER.length - 1],
            ]);

            if(result.rows.length == 0) {
                return null;
            }

            const first = result.rows[0];
            const polygons = result.rows.map(row => this.toLatitudeLongitudeRing(row.coordinates));
            const totalRings = parseInt(first.total_rings, 10);

            return {
                gid: gid,
                level: first.level,
                name: first.name,
                region: first.region,
                country: first.country,
                polygons: polygons,
                points: polygons.reduce((sum, ring) => sum + ring.length, 0),
                rings: {
                    returned: polygons.length,
                    total: totalRings,
                    //Islands too small to matter, and rings beyond maxRings. Worth reporting:
                    //it is the difference between "this is the area" and "this is most of it".
                    omitted: totalRings - polygons.length,
                },
            };
        }
        finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    /*
    * Function: toLatitudeLongitudeRing
    * GeoJSON is [longitude, latitude] and closes its rings; the map filter is
    * [latitude, longitude] and closes them itself. Both are corrected here rather than in
    * the client, so every caller gets coordinates it can pass straight to the query API.
    */
    toLatitudeLongitudeRing(coordinates) {
        const ring = (coordinates || []).map(point => [point[1], point[0]]);
        if(ring.length > 1) {
            const first = ring[0];
            const last = ring[ring.length - 1];
            if(first[0] == last[0] && first[1] == last[1]) {
                ring.pop();
            }
        }
        return ring;
    }

    resolveLevels(level) {
        if(level === undefined || level === "") {
            return Gadm.LEVELS;
        }
        const parsed = Number.parseInt(level, 10);
        if(!Gadm.LEVELS.includes(parsed)) {
            return false;
        }
        return [parsed];
    }

    clampInteger(value, fallback, min, max) {
        const parsed = Number.parseInt(value, 10);
        if(!Number.isInteger(parsed)) {
            return fallback;
        }
        return Math.max(min, Math.min(parsed, max));
    }

    //GADM identifiers look like SWE, SWE.13_1, SWE.18.12_1
    isValidGid(gid) {
        return /^[A-Za-z0-9._-]{3,32}$/.test(gid);
    }
}

export default Gadm;
