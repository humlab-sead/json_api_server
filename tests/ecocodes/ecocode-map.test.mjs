/**
 * The ecocode values behind the result map's ecocode layers (EcoCodes.aggregateSiteEcoCodes and friends): how sample ages are
 * taken from the age summary, how a time window picks samples, and the reference scale of each ecocode. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import EcoCodes from "../../src/EndpointModules/EcoCodes.class.js";

const app = {
    expressApp: { get() {}, post() {} },
};
const ecoCodes = new EcoCodes(app);

function sample(siteId, sampleId, individuals, ecocodes, older = null, younger = null) {
    return { site_id: siteId, physical_sample_id: sampleId, individuals, ecocodes, older, younger };
}

test("a sample spans all of its ages, and an open bound counts as the bound it has", () => {
    const samples = new Map([
        [1, sample(10, 1, 5, {})],
        [2, sample(10, 2, 5, {})],
        [3, sample(10, 3, 5, {})],
    ]);
    ecoCodes.applySampleAges(samples, {
        datings: [
            { method_id: 151, ages: [{ physical_sample_id: 1, older: 3000, younger: 2500 }] },
            { method_id: 155, ages: [
                { physical_sample_id: 1, older: 2800, younger: 2000 },
                { physical_sample_id: 2, older: null, younger: 4000 },
                { physical_sample_id: 99, older: 100, younger: 50 },
            ] },
        ],
    });

    assert.deepEqual([samples.get(1).older, samples.get(1).younger], [3000, 2000]);
    assert.deepEqual([samples.get(2).older, samples.get(2).younger], [4000, 4000]);
    assert.deepEqual([samples.get(3).older, samples.get(3).younger], [null, null]);
});

test("the time window is [younger, older] in years BP, in either order", () => {
    assert.equal(ecoCodes.parseTimeWindow(null), null);
    assert.equal(ecoCodes.parseTimeWindow(undefined), null);
    assert.deepEqual(ecoCodes.parseTimeWindow([5000, 1000]), { younger: 1000, older: 5000 });
    assert.deepEqual(ecoCodes.parseTimeWindow(["-50", "200"]), { younger: -50, older: 200 });
    assert.equal(ecoCodes.parseTimeWindow([1000]), false);
    assert.equal(ecoCodes.parseTimeWindow([1000, "x"]), false);
    assert.equal(ecoCodes.parseTimeWindow("1000-5000"), false);
});

test("a site sums its samples, and a time window keeps only the samples dated to within it", () => {
    const samplesBySite = ecoCodes.groupSampleVectorsBySite([
        sample(10, 1, 20, { 323: 10, 328: 5 }, 3000, 2000),
        sample(10, 2, 30, { 328: 25 }, 9000, 8000),
        sample(10, 3, 7, { 323: 7 }),
        sample(20, 4, 4, { 326: 4 }, 12000, 11000),
    ]);

    const all = ecoCodes.aggregateSiteEcoCodes(samplesBySite, [10, 20, 30]);
    assert.equal(all.time_window, null);
    assert.deepEqual(all.sample_counts, { included: 4, undated: 0, outside_time_window: 0 });
    assert.deepEqual(all.sites.find(site => site.site_id == 10), {
        site_id: 10, samples: 3, individuals: 57, ecocodes: { 323: 17, 328: 30 },
    });

    const windowed = ecoCodes.aggregateSiteEcoCodes(samplesBySite, [10, 20], { younger: 2500, older: 8500 });
    assert.deepEqual(windowed.time_window, [2500, 8500]);
    assert.deepEqual(windowed.sample_counts, { included: 2, undated: 1, outside_time_window: 1 });
    assert.deepEqual(windowed.sites, [
        { site_id: 10, samples: 2, individuals: 50, ecocodes: { 323: 10, 328: 30 } },
    ]);
});

test("the reference scale counts sites lacking an ecocode as 0 and leaves out small sites", () => {
    const samplesBySite = ecoCodes.groupSampleVectorsBySite([
        sample(1, 1, 100, { 323: 50, 328: 50 }),
        sample(2, 2, 100, { 328: 100 }),
        sample(3, 3, 100, { 328: 25, 323: 75 }),
        sample(4, 4, 5, { 323: 5 }),
    ]);

    const reference = ecoCodes.compileMapReference(samplesBySite);
    assert.equal(reference.site_count, 3);
    assert.equal(reference.min_individuals, 30);
    assert.ok(Math.abs(reference.ecocodes[323].mean - (0.5 + 0 + 0.75) / 3) < 1e-9);
    assert.equal(reference.ecocodes[323].p95, 0.75);
    assert.equal(reference.ecocodes[328].p95, 1);
});
