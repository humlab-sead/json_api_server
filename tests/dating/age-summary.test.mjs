/**
 * The site age summary (DatingModule.compileAgeSummary): how each kind of dating is normalized to years BP and
 * how datings are combined per dating method. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import DatingModule from "../../src/DataFetchingModules/DatingModule.class.js";

const methods = [
    { method_id: 151, method_name: "C14 Conventional" },
    { method_id: 147, method_name: "Uranium-Series (general)" },
    { method_id: 134, method_name: "Stratigraphic (Geological)" },
    { method_id: 174, method_name: "Composite chronology" },
    { method_id: 171, method_name: "Petrographic microscopy" },
];
const app = {
    expressApp: null,
    getMethodByMethodId(site, id) { return site.lookup_tables.methods.find(m => m.method_id == id) || false; },
};
const datingModule = new DatingModule(app);

function geochronology(age, errorOlder, errorYounger, uncertainty = null) {
    return { geochron_id: 1, age, error_older: errorOlder, error_younger: errorYounger, dating_uncertainty: uncertainty };
}

function relativeDate(values) {
    return { relative_date_id: 1, method_id: null, cal_age_older: null, cal_age_younger: null, c14_age_older: null, c14_age_younger: null, ...values };
}

/** A site as it is when compileAgeSummary runs: entities on the datasets, no dendro data groups. */
function site(datasets) {
    return {
        lookup_tables: { methods },
        data_groups: [],
        datasets: datasets.map((dataset, i) => ({
            dataset_id: i + 1,
            method_id: dataset.method_id,
            analysis_entities: dataset.entities.map((entity, j) => ({ analysis_entity_id: (i + 1) * 100 + j, physical_sample_id: j, dataset_id: i + 1, ...entity })),
        })),
    };
}

test("geochronology: age plus error_older is older, age minus error_younger is younger", () => {
    const age = datingModule.normalizeGeochronologyAge(geochronology("48200.00000", "9600.00000", "4400.00000"), 151);
    assert.equal(age.older, 57800);
    assert.equal(age.younger, 43800);
    assert.equal(age.radiocarbon_years, true);
});

test("geochronology: missing errors give a single year, not NaN", () => {
    const age = datingModule.normalizeGeochronologyAge(geochronology("127000.00000", null, null), 147);
    assert.equal(age.older, 127000);
    assert.equal(age.younger, 127000);
    assert.equal(age.radiocarbon_years, false);
});

test("geochronology: '>' and 'To' are open toward older, '<' and 'From' toward younger", () => {
    for (const [uncertainty, openOlder, openYounger] of [[">", true, false], ["To", true, false], ["<", false, true], ["From", false, true], ["Ca.", false, false]]) {
        const age = datingModule.normalizeGeochronologyAge(geochronology("50300", null, null, uncertainty), 151);
        assert.equal(age.open_older, openOlder, uncertainty);
        assert.equal(age.open_younger, openYounger, uncertainty);
    }
});

test("relative date: calendar values are preferred over radiocarbon values", () => {
    const age = datingModule.normalizeRelativeAge(relativeDate({ cal_age_older: "1907", cal_age_younger: "1540", c14_age_older: "1900", c14_age_younger: "1600" }));
    assert.deepEqual([age.older, age.younger, age.radiocarbon_years], [1907, 1540, false]);
});

test("relative date: radiocarbon values are used, and flagged, when there are no calendar values", () => {
    const age = datingModule.normalizeRelativeAge(relativeDate({ c14_age_older: "122000.00000", c14_age_younger: "45000.00000" }));
    assert.deepEqual([age.older, age.younger, age.radiocarbon_years], [122000, 45000, true]);
});

test("relative date: 0 - 0 is a placeholder and gives no age", () => {
    assert.equal(datingModule.normalizeRelativeAge(relativeDate({ c14_age_older: "0.00000", c14_age_younger: "0.00000" })), null);
    assert.equal(datingModule.normalizeRelativeAge(relativeDate({})), null);
});

test("relative date: a range ending at 0 BP is kept", () => {
    const age = datingModule.normalizeRelativeAge(relativeDate({ c14_age_older: "10000", c14_age_younger: "0" }));
    assert.deepEqual([age.older, age.younger], [10000, 0]);
});

test("relative date: a missing bound is open, except for a single calendar date", () => {
    const range = datingModule.normalizeRelativeAge(relativeDate({ relative_age_type_id: 13, cal_age_older: "1808" }));
    assert.deepEqual([range.older, range.younger, range.open_older, range.open_younger], [1808, 1808, false, true]);
    const single = datingModule.normalizeRelativeAge(relativeDate({ relative_age_type_id: 12, cal_age_older: "1808" }));
    assert.deepEqual([single.older, single.younger, single.open_older, single.open_younger], [1808, 1808, false, false]);
});

test("relative date: a reversed range is swapped and flagged", () => {
    const age = datingModule.normalizeRelativeAge(relativeDate({ cal_age_older: "2500", cal_age_younger: "2550" }));
    assert.deepEqual([age.older, age.younger, age.swapped], [2550, 2500, true]);
});

test("entity age: a modelled range in calendar years BP", () => {
    const age = datingModule.normalizeEntityAge({ age: null, age_older: "15210.00000", age_younger: "14648.00000", dating_specifier: "Chosen_C14" });
    assert.deepEqual([age.older, age.younger, age.radiocarbon_years], [15210, 14648, false]);
    assert.equal(datingModule.normalizeEntityAge({ age: null, age_older: null, age_younger: null }), null);
});

test("summary: one dating per method, spanning its oldest to its youngest age", () => {
    const summary = datingModule.compileAgeSummary(site([
        { method_id: 151, entities: [
            { dating_values: geochronology("35600", "1900", "1600") },
            { dating_values: geochronology("48200", "9600", "4400") },
            { dating_values: geochronology("50300", null, null, ">") },
        ] },
        { method_id: 134, entities: [{ dating_values: relativeDate({ c14_age_older: "122000", c14_age_younger: "45000" }) }] },
        { method_id: 174, entities: [{ entity_ages: { age_older: "43678", age_younger: "36558" } }] },
    ]));
    const byName = Object.fromEntries(summary.datings.map(d => [d.method_name, d]));
    assert.deepEqual([byName["C14 Conventional"].older, byName["C14 Conventional"].younger], [57800, 34000]);
    assert.equal(byName["C14 Conventional"].age_count, 3);
    assert.equal(byName["C14 Conventional"].radiocarbon_years_count, 3);
    //The '>' age is not at either end of the span, so the span itself is not open
    assert.equal(byName["C14 Conventional"].open_older, false);
    assert.deepEqual([byName["Composite chronology"].older, byName["Composite chronology"].younger], [43678, 36558]);
    assert.deepEqual([summary.older, summary.younger], [122000, 34000]);
    assert.deepEqual(summary.datings.map(d => d.method_name), ["Stratigraphic (Geological)", "C14 Conventional", "Composite chronology"]);
});

test("summary: an open-ended age at the end of a span makes the span open", () => {
    const summary = datingModule.compileAgeSummary(site([
        { method_id: 147, entities: [{ dating_values: geochronology("127000", null, null, ">") }] },
    ]));
    assert.equal(summary.datings[0].open_older, true);
    assert.equal(summary.datings[0].open_younger, false);
});

test("summary: a relative date that names its own dating method is grouped under that method", () => {
    const summary = datingModule.compileAgeSummary(site([
        { method_id: 171, entities: [{ dating_values: relativeDate({ method_id: 128, relative_date_method_name: "Archaeological period calendar years", cal_age_older: "1950", cal_age_younger: "1550" }) }] },
    ]));
    assert.equal(summary.datings[0].method_id, 128);
    assert.equal(summary.datings[0].method_name, "Archaeological period calendar years");
});

test("summary: a site without datings has an empty summary", () => {
    assert.deepEqual(datingModule.compileAgeSummary(site([])), { bp_year: 1950, older: null, younger: null, datings: [] });
});
