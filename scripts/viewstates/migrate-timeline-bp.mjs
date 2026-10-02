#!/usr/bin/env node
/**
 * Moves the Timeline filter's selections in saved viewstates into years BP.
 *
 *   node scripts/viewstates/migrate-timeline-bp.mjs --saved-before <ISO date or epoch ms> [--apply]
 *
 * Clients before the timeline BP alignment saved the Timeline filter
 * (analysis_entity_ages) as its raw slider values [a, b]: years BP negated, older
 * end first. The user saw -b ... -a BP. The client now saves [younger, older] in
 * plain years BP, so each selection becomes [-b, -a] - the period the user chose.
 * Its result set changes, since the old one was the bug's (see
 * plans/timeline-bp-alignment-plan.md in sead-deployment, option A).
 *
 * A selection that is the untouched default 5.33M year scale is dropped rather than
 * converted. The old client sent it as picks without the user touching the slider,
 * so it was never a choice.
 *
 * --saved-before must be when the new client was deployed: viewstates saved after
 * that are already in BP. Migrated viewstates are marked, so running it again
 * leaves them alone. Without --apply it only lists what it would change.
 *
 * Reads the same MONGO_HOST / _USER / _PASS / _DB as the server, so run it in the
 * container:
 *
 *   podman compose exec json_api_server node scripts/viewstates/migrate-timeline-bp.mjs --saved-before 2026-10-15T12:00:00Z
 */
import { MongoClient } from "mongodb";

const USAGE = `Usage:
  migrate-timeline-bp.mjs --saved-before <ISO date or epoch ms> [--apply]`;

const FACET_CODE = "analysis_entity_ages";
const MIGRATION = "timeline-bp";
//The old client's default scale, older end, in slider values (years BP negated)
const DEFAULT_SCALE_OLDER = -5330000;

function fail(message) {
    console.error(message);
    console.error("");
    console.error(USAGE);
    process.exit(2);
}

function parseArgs(argv) {
    const args = { apply: false, savedBefore: null };
    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === "--apply") {
            args.apply = true;
        }
        else if (argv[i] === "--saved-before") {
            const value = argv[++i];
            const time = /^\d+$/.test(value || "") ? Number(value) : Date.parse(value);
            if (Number.isNaN(time)) fail(`"${value}" is not a date.`);
            args.savedBefore = time;
        }
        else {
            fail(`Unknown argument "${argv[i]}".`);
        }
    }
    if (args.savedBefore === null) fail("--saved-before is needed.");
    return args;
}

//The slider's "now" is the save year in BP, negated, so the default scale moves by a year each year
function isUntouchedDefault([older, younger], saved) {
    const nowBP = new Date(saved).getUTCFullYear() - 1950;
    return Math.abs(younger - nowBP) <= 1 && older === DEFAULT_SCALE_OLDER + younger;
}

/*
 * Returns the viewstate's facets with the Timeline converted, and what was done to each
 * Timeline entry, or null if the viewstate has no Timeline selection.
 */
function migrateFacets(viewstate) {
    if (!Array.isArray(viewstate.facets)) return null;
    const changes = [];
    const facets = [];
    for (const facet of viewstate.facets) {
        if (facet.name !== FACET_CODE || !Array.isArray(facet.selections) || facet.selections.length !== 2) {
            facets.push(facet);
            continue;
        }
        const [a, b] = facet.selections.map(Number);
        if (isUntouchedDefault([a, b], viewstate.saved)) {
            changes.push(`[${a}, ${b}] is the untouched default scale, dropped`);
            continue;
        }
        const selections = [-b, -a];
        changes.push(`[${a}, ${b}] -> [${selections.join(", ")}] (${selections[0]} - ${selections[1]} BP)`);
        facets.push({ ...facet, selections });
    }
    return changes.length > 0 ? { facets, changes } : null;
}

const args = parseArgs(process.argv.slice(2));

for (const name of ["MONGO_HOST", "MONGO_USER", "MONGO_PASS", "MONGO_DB"]) {
    if (!process.env[name]) fail(`${name} is not set. Run this in the json_api_server container.`);
}
const client = new MongoClient(`mongodb://${encodeURIComponent(process.env.MONGO_USER)}:${encodeURIComponent(process.env.MONGO_PASS)}@${process.env.MONGO_HOST}:27017/`);
try {
    await client.connect();
    const viewstates = client.db(process.env.MONGO_DB).collection("viewstates");
    const candidates = await viewstates.find({
        "facets.name": FACET_CODE,
        saved: { $lt: args.savedBefore },
        migrations: { $ne: MIGRATION },
    }).sort({ saved: 1 }).toArray();

    let migrated = 0;
    for (const viewstate of candidates) {
        const result = migrateFacets(viewstate);
        if (!result) continue;
        console.log(`${viewstate.id}  saved ${new Date(viewstate.saved).toISOString()}  ${viewstate.name || ""}`);
        for (const change of result.changes) console.log(`    ${change}`);
        if (args.apply) {
            await viewstates.updateOne(
                { _id: viewstate._id },
                { $set: { facets: result.facets }, $addToSet: { migrations: MIGRATION } }
            );
        }
        migrated++;
    }

    const total = await viewstates.countDocuments({ "facets.name": FACET_CODE });
    console.log("");
    console.log(`${total} viewstates use the Timeline filter; ${migrated} ${args.apply ? "migrated" : "would be migrated (dry run, --apply to write)"}.`);
}
finally {
    await client.close();
}
