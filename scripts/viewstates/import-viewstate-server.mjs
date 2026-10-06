#!/usr/bin/env node
/**
 * Copies the viewstates saved through the old viewstate server (sqs_viewstate_server)
 * into this server's viewstates collection.
 *
 *   node scripts/viewstates/import-viewstate-server.mjs --from-db <database> [--from-collection viewstate] [--apply]
 *
 * The old server kept them in its own collection, "viewstate", with each owner stored
 * as sha1(Google email + its salt). This server keys Google users on their bare email
 * and stores sha1(user id + JAS_AUTH_SALT) (AuthIdentity.userIdOf, Viewstates.getUserToken),
 * so the owners are copied as they are: with the old server's salt as JAS_AUTH_SALT,
 * each Google user finds what they saved there. Anonymised owners ("deleted") are copied
 * too, since a viewstate's link keeps working after its owner is removed.
 *
 * The old server's delete replaced the whole document with { user: "deleted" }, so
 * documents without an id hold nothing to restore and are skipped. A viewstate whose
 * id is already here is left alone, so running it again copies nothing twice. Without
 * --apply it only counts what it would copy.
 *
 * The old database has to be on this server's Mongo; restore a dump of it there first
 * if it is not. Reads the same MONGO_HOST / _USER / _PASS / _DB as the server, so run
 * it in the container:
 *
 *   podman compose exec json_api_server node scripts/viewstates/import-viewstate-server.mjs --from-db sead_viewstates
 *
 * The copied viewstates are from before the timeline BP alignment, so run
 * migrate-timeline-bp.mjs after it.
 */
import { MongoClient } from "mongodb";

const USAGE = `Usage:
  import-viewstate-server.mjs --from-db <database> [--from-collection viewstate] [--apply]`;

const MIGRATION = "viewstate-server";

function fail(message) {
    console.error(message);
    console.error("");
    console.error(USAGE);
    process.exit(2);
}

function parseArgs(argv) {
    const args = { apply: false, fromDb: null, fromCollection: "viewstate" };
    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === "--apply") {
            args.apply = true;
        }
        else if (argv[i] === "--from-db" || argv[i] === "--from-collection") {
            const value = argv[++i];
            if (!value || value.startsWith("--")) fail(`${argv[i - 1]} needs a name.`);
            args[argv[i - 1] === "--from-db" ? "fromDb" : "fromCollection"] = value;
        }
        else {
            fail(`Unknown argument "${argv[i]}".`);
        }
    }
    if (args.fromDb === null) fail("--from-db is needed.");
    return args;
}

const args = parseArgs(process.argv.slice(2));

for (const name of ["MONGO_HOST", "MONGO_USER", "MONGO_PASS", "MONGO_DB"]) {
    if (!process.env[name]) fail(`${name} is not set. Run this in the json_api_server container.`);
}
if (!process.env.JAS_AUTH_SALT) {
    console.warn("JAS_AUTH_SALT is not set here. Google users only find these viewstates once it is the old server's salt.");
}
const client = new MongoClient(`mongodb://${encodeURIComponent(process.env.MONGO_USER)}:${encodeURIComponent(process.env.MONGO_PASS)}@${process.env.MONGO_HOST}:27017/`);
try {
    await client.connect();
    const source = client.db(args.fromDb).collection(args.fromCollection);
    const target = client.db(process.env.MONGO_DB).collection("viewstates");
    if (await source.estimatedDocumentCount() === 0) {
        fail(`${args.fromDb}.${args.fromCollection} is empty or does not exist.`);
    }

    const counts = { copied: 0, present: 0, noId: 0 };
    const owners = new Set();
    const seen = new Set();
    for await (const viewstate of source.find({}).sort({ saved: 1 })) {
        if (!viewstate.id) {
            counts.noId++;
            continue;
        }
        if (seen.has(viewstate.id) || await target.countDocuments({ id: viewstate.id }, { limit: 1 }) > 0) {
            counts.present++;
            continue;
        }
        seen.add(viewstate.id);
        const { _id, ...copy } = viewstate;
        if (args.apply) {
            await target.insertOne({ ...copy, migrations: [...(copy.migrations || []), MIGRATION] });
        }
        if (copy.user && copy.user !== "deleted") owners.add(copy.user);
        counts.copied++;
    }

    console.log(`${args.fromDb}.${args.fromCollection} -> ${process.env.MONGO_DB}.viewstates`);
    console.log(`${counts.copied} viewstates ${args.apply ? "copied" : "would be copied (dry run, --apply to write)"}, owned by ${owners.size} users.`);
    console.log(`${counts.present} already here, left alone. ${counts.noId} deleted by the old server (no id), skipped.`);
    if (args.apply && counts.copied > 0) {
        console.log("");
        console.log("Next: migrate-timeline-bp.mjs, as these were saved by clients from before the timeline BP alignment.");
    }
}
finally {
    await client.close();
}
