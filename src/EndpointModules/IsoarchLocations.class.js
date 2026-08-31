class IsoarchLocations {
    constructor(app) {
        this.app = app;
        this.setupEndpoints();
    }

    setupEndpoints() {
        this.app.expressApp.get('/isoarch/locations', async (req, res) => {
            const requestedLimit = Number.parseInt(req.query.limit, 10);
            const limit = Number.isInteger(requestedLimit)
                ? Math.max(1, Math.min(requestedLimit, 5000))
                : 5000;

            const requestedPage = Number.parseInt(req.query.page, 10);
            const page = Number.isInteger(requestedPage) ? Math.max(1, requestedPage) : 1;

            const filter = {};
            if (req.query.country) {
                filter.country = req.query.country;
            }
            if (req.query.coordinates_type) {
                filter.coordinates_type = req.query.coordinates_type;
            }

            try {
                const col = this.app.mongo.collection('isoarch_locations');
                const [locations, total] = await Promise.all([
                    col.find(filter, { projection: { _id: 0 } })
                       .skip((page - 1) * limit)
                       .limit(limit)
                       .toArray(),
                    col.countDocuments(filter),
                ]);

                res.header('Content-type', 'application/json');
                res.send(JSON.stringify({ total, page, limit, locations }, null, 2));
            }
            catch (error) {
                console.error('IsoarchLocations error:', error);
                res.status(500).send(JSON.stringify({ error: 'Internal server error' }, null, 2));
            }
        });

        this.app.expressApp.get('/isoarch/location/:location_name', async (req, res) => {
            const locationName = decodeURIComponent(req.params.location_name);
            const db = this.app.mongo;

            try {
                // Wave 1: location records + all materials at this location
                const [locations, materials] = await Promise.all([
                    db.collection('isoarch_locations')
                      .find({ location_name: locationName }, { projection: { _id: 0 } })
                      .toArray(),
                    db.collection('isoarch_materials')
                      .find({ location_name: locationName }, { projection: { _id: 0 } })
                      .toArray(),
                ]);

                if (locations.length === 0 && materials.length === 0) {
                    res.status(404).send(JSON.stringify({ error: 'Location not found' }, null, 2));
                    return;
                }

                const materialIds = materials.map(m => m.material_identifier);

                // Parse semicolon-separated short_references from all materials
                const refKeys = [...new Set(
                    materials.flatMap(m =>
                        (m.short_references || '').split(';').map(s => s.trim()).filter(Boolean)
                    )
                )];

                // Wave 2: samples + references (both depend only on Wave 1 results)
                const [samples, references] = await Promise.all([
                    db.collection('isoarch_samples')
                      .find({ material_identifier: { $in: materialIds } }, { projection: { _id: 0 } })
                      .toArray(),
                    refKeys.length > 0
                        ? db.collection('isoarch_references')
                             .find({ short_reference: { $in: refKeys } }, { projection: { _id: 0 } })
                             .toArray()
                        : Promise.resolve([]),
                ]);

                const sampleIds = samples.map(s => s.sample_identifier);

                // Wave 3: all measure types, keyed by sample_identifier
                const [organicMeasures, mineralMeasures, radiocarbonMeasures] = await Promise.all([
                    db.collection('isoarch_organic_measures')
                      .find({ sample_identifier: { $in: sampleIds } }, { projection: { _id: 0, _row_hash: 0 } })
                      .toArray(),
                    db.collection('isoarch_mineral_measures')
                      .find({ sample_identifier: { $in: sampleIds } }, { projection: { _id: 0, _row_hash: 0 } })
                      .toArray(),
                    db.collection('isoarch_radiocarbon_measures')
                      .find({ sample_identifier: { $in: sampleIds } }, { projection: { _id: 0, _row_hash: 0 } })
                      .toArray(),
                ]);

                // Wave 4: datasets referenced by any measure
                const datasetDois = [...new Set([
                    ...organicMeasures.map(m => m.dataset_doi),
                    ...mineralMeasures.map(m => m.dataset_doi),
                    ...radiocarbonMeasures.map(m => m.dataset_doi),
                ].filter(Boolean))];

                const datasets = datasetDois.length > 0
                    ? await db.collection('isoarch_datasets')
                         .find({ dataset_doi: { $in: datasetDois } }, { projection: { _id: 0 } })
                         .toArray()
                    : [];

                // ── Assemble nested structure ──────────────────────────────────

                // Group samples and measures by their parent IDs
                const samplesByMaterial = groupBy(samples, 'material_identifier');
                const organicBySample   = groupBy(organicMeasures, 'sample_identifier');
                const mineralBySample   = groupBy(mineralMeasures, 'sample_identifier');
                const radioBySample     = groupBy(radiocarbonMeasures, 'sample_identifier');

                const assembledMaterials = materials.map(mat => ({
                    ...stripNulls(mat),
                    samples: (samplesByMaterial.get(mat.material_identifier) || []).map(samp => ({
                        ...stripNulls(samp),
                        organic_measures:     (organicBySample.get(samp.sample_identifier) || []).map(stripNulls),
                        mineral_measures:     (mineralBySample.get(samp.sample_identifier) || []).map(stripNulls),
                        radiocarbon_measures: (radioBySample.get(samp.sample_identifier) || []).map(stripNulls),
                    })),
                }));

                res.header('Content-type', 'application/json');
                res.send(JSON.stringify({
                    location_name: locationName,
                    locations,
                    materials: assembledMaterials,
                    references,
                    datasets,
                }, null, 2));
            }
            catch (error) {
                console.error('IsoarchLocation error:', error);
                res.status(500).send(JSON.stringify({ error: 'Internal server error' }, null, 2));
            }
        });
    }
}

function groupBy(arr, key) {
    const map = new Map();
    for (const item of arr) {
        const k = item[key];
        if (!map.has(k)) map.set(k, []);
        map.get(k).push(item);
    }
    return map;
}

function stripNulls(obj) {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
        if (v !== null) out[k] = v;
    }
    return out;
}

export default IsoarchLocations;
