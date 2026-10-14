/**
 * The Neotoma site harvest (NeotomaSites.normalizeSites): how Neotoma's site listing becomes one plottable
 * document per site. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import NeotomaSites from "../../src/EndpointModules/NeotomaSites.class.js";

const harvestedAt = new Date("2026-10-14T12:00:00Z");

function geography(type, coordinates) {
    return JSON.stringify({ type, crs: { type: "name", properties: { name: "EPSG:4326" } }, coordinates });
}

function unit(collectionunitid, datasetid, datasettype) {
    return { handle: "H"+collectionunitid, collectionunit: "Core "+collectionunitid, collectionunitid, collectionunittype: "Core", datasets: [{ datasetid, datasettype }] };
}

test("a point site keeps its position", () => {
    const point = NeotomaSites.representativePoint(geography("Point", [-75.25, 55.09167]));
    assert.deepEqual(point, { longitude: -75.25, latitude: 55.09167, geometryType: "Point" });
});

test("a polygon site is placed at the middle of its bounding box", () => {
    const point = NeotomaSites.representativePoint(geography("Polygon", [[[10, 60], [12, 60], [12, 61], [10, 61], [10, 60]]]));
    assert.deepEqual(point, { longitude: 11, latitude: 60.5, geometryType: "Polygon" });
});

test("a missing, unparsable or out of range geography gives no position", () => {
    assert.equal(NeotomaSites.representativePoint(null), null);
    assert.equal(NeotomaSites.representativePoint("{not json"), null);
    assert.equal(NeotomaSites.representativePoint(geography("Polygon", [])), null);
    assert.equal(NeotomaSites.representativePoint(geography("Point", [200, 10])), null);
});

test("a collection unit listed once per dataset becomes one unit with all its datasets", () => {
    const [site] = NeotomaSites.normalizeSites([{
        siteid: 7, sitename: "Lake", sitedescription: "A lake.", altitude: 12, geography: geography("Point", [15, 62]),
        collectionunits: [unit(1, 10, "pollen"), unit(1, 11, "geochronologic"), unit(1, 10, "pollen"), unit(2, 12, "charcoal")]
    }], harvestedAt);

    assert.equal(site.collectionunits.length, 2);
    assert.deepEqual(site.collectionunits[0].datasets, [
        { datasetid: 10, datasettype: "pollen" },
        { datasetid: 11, datasettype: "geochronologic" }
    ]);
    assert.deepEqual(site.collectionunits[1].datasets, [{ datasetid: 12, datasettype: "charcoal" }]);
    assert.equal(site.harvested_at, harvestedAt);
});

test("units and datasets without ids are dropped", () => {
    const [site] = NeotomaSites.normalizeSites([{
        siteid: 7, sitename: "Lake", geography: geography("Point", [15, 62]),
        collectionunits: [
            { collectionunitid: null, datasets: [{ datasetid: null, datasettype: null }] },
            { collectionunitid: 3, collectionunit: "C", datasets: [{ datasetid: null, datasettype: null }] }
        ]
    }], harvestedAt);

    assert.equal(site.collectionunits.length, 1);
    assert.deepEqual(site.collectionunits[0].datasets, []);
});

test("rows without a site id or a position are left out, and a repeated site is merged", () => {
    const sites = NeotomaSites.normalizeSites([
        { siteid: null, geography: null, collectionunits: [unit(9, 90, "diatom")] },
        { siteid: 5, sitename: "Nowhere", geography: null, collectionunits: [] },
        { siteid: 6, sitename: "Bog", geography: geography("Point", [1, 2]), collectionunits: [unit(1, 10, "pollen")] },
        { siteid: 6, sitename: "Bog", geography: geography("Point", [1, 2]), collectionunits: [unit(2, 20, "insect")] }
    ], harvestedAt);

    assert.deepEqual(sites.map(s => s.siteid), [6]);
    assert.deepEqual(sites[0].collectionunits.map(u => u.collectionunitid), [1, 2]);
});
