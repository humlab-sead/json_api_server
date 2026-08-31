#!/usr/bin/env node
/**
 * Compares two directories of captured site payloads.
 *
 * Comparison is structural rather than textual: arrays are compared as multisets
 * (ordering in the current implementation is unspecified — most queries have no
 * ORDER BY) and object key order is ignored, but value *types* are compared
 * strictly so that a `numeric` column silently changing from the string "12.5"
 * to the number 12.5 is reported rather than hidden.
 *
 * Fields that are expected to differ between runs (server version stamps, and
 * anything listed in --ignore) are excluded before comparison.
 *
 * Usage:
 *   node scripts/compare-site-baseline.mjs baseline/main baseline/after
 *   node scripts/compare-site-baseline.mjs baseline/run1 baseline/run2 --ordered
 *   node scripts/compare-site-baseline.mjs a b --ignore lookup_tables.biblio
 *
 * Exit code is 0 when every site matches, 1 otherwise.
 */

import fs from 'fs';
import path from 'path';
import { diffValues, formatDiff } from './lib/site-diff.mjs';

/** Volatile fields that carry no data and would otherwise mask real diffs. */
const ALWAYS_IGNORED = [
    "api_source",
    "server_version",
];

function parseArgs(argv) {
    const args = { ignore: [], ordered: false, maxGroups: 40 };
    const positional = [];
    for(let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        if(arg == "--ignore") args.ignore.push(argv[++i]);
        else if(arg == "--ordered") args.ordered = true;
        else if(arg == "--max-groups") args.maxGroups = parseInt(argv[++i]);
        else positional.push(arg);
    }
    if(positional.length != 2) throw new Error("Usage: compare-site-baseline.mjs <dirA> <dirB> [--ordered] [--ignore path]");
    args.expectedDir = positional[0];
    args.actualDir = positional[1];
    return args;
}

/**
 * Removes ignored paths from a payload. Paths are dot-separated and are matched
 * against the top level of the site object, e.g. "lookup_tables.biblio".
 */
function stripIgnored(payload, ignorePaths) {
    if(payload === null || typeof payload != "object") return payload;
    const clone = JSON.parse(JSON.stringify(payload));
    for(const ignorePath of ignorePaths) {
        const parts = ignorePath.split(".");
        let node = clone;
        for(let i = 0; i < parts.length - 1 && node; i++) {
            node = node[parts[i]];
        }
        if(node && typeof node == "object") {
            delete node[parts[parts.length - 1]];
        }
    }
    return clone;
}

function listSiteFiles(dir) {
    return fs.readdirSync(dir)
        .filter(name => name.endsWith(".json") && !name.startsWith("_"))
        .sort((a, b) => parseInt(a) - parseInt(b));
}

function main() {
    const args = parseArgs(process.argv.slice(2));
    const ignorePaths = ALWAYS_IGNORED.concat(args.ignore);

    const expectedFiles = listSiteFiles(args.expectedDir);
    const actualFiles = listSiteFiles(args.actualDir);
    const onlyExpected = expectedFiles.filter(f => !actualFiles.includes(f));
    const onlyActual = actualFiles.filter(f => !expectedFiles.includes(f));

    console.log("Comparing " + args.expectedDir + "  ->  " + args.actualDir);
    console.log("Mode: " + (args.ordered ? "order-sensitive" : "order-insensitive (arrays as multisets)"));
    console.log("Ignored: " + ignorePaths.join(", "));
    console.log("");

    if(onlyExpected.length) console.log("Only in " + args.expectedDir + ": " + onlyExpected.join(", "));
    if(onlyActual.length) console.log("Only in " + args.actualDir + ": " + onlyActual.join(", "));

    let failures = 0;
    const shared = expectedFiles.filter(f => actualFiles.includes(f));
    for(const file of shared) {
        const expected = stripIgnored(JSON.parse(fs.readFileSync(path.join(args.expectedDir, file), 'utf8')), ignorePaths);
        const actual = stripIgnored(JSON.parse(fs.readFileSync(path.join(args.actualDir, file), 'utf8')), ignorePaths);
        const groups = diffValues(expected, actual, { sortArrays: !args.ordered });
        const siteId = path.basename(file, ".json");
        if(groups.length == 0) {
            console.log("site " + siteId + ": OK");
        }
        else {
            failures++;
            console.log("site " + siteId + ": DIFFERS");
            console.log(formatDiff(groups, { maxGroups: args.maxGroups }));
        }
    }

    console.log("");
    console.log(shared.length - failures + "/" + shared.length + " site(s) matched");
    if(onlyExpected.length || onlyActual.length) failures++;
    process.exit(failures == 0 ? 0 : 1);
}

main();
