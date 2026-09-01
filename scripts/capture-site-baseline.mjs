#!/usr/bin/env node
/**
 * Captures site payloads from a running JAS instance into a directory of JSON
 * files, one per site, for later comparison by compare-site-baseline.mjs.
 *
 * Caching is always bypassed (noCache=true in the URL) so that what is captured
 * is the output of the fetch path under test rather than a stale Mongo document.
 *
 * Usage:
 *   node scripts/capture-site-baseline.mjs --out baseline/main
 *   node scripts/capture-site-baseline.mjs --out baseline/old --method true
 *   node scripts/capture-site-baseline.mjs --out baseline/run2 --sites 1,2,79
 *
 * Options:
 *   --out <dir>        output directory (required)
 *   --base <url>       server base URL (default http://localhost:8485)
 *   --sites <list>     comma-separated site ids (default: the standard test set)
 *   --method true      fetch via the original per-row getSite() instead of
 *                      the default consolidated implementation
 *   --timeout <ms>     per-site timeout (default 600000)
 */

import fs from 'fs';
import path from 'path';

/**
 * The standard test set. Chosen to cover every data fetching module that can
 * actually fire, plus every size class and the known edge cases.
 * (IsotopeModule has an empty method allowlist and therefore never runs.)
 */
export const DEFAULT_SITES = [
    1,      // mid-size, mixed methods
    2,      // mid-size, 30 datasets, mixed methods
    26,     // edge case: no sample groups at all
    76,     // 1286 samples, 5855 analysis entities, abundances + measured values
    79,     // largest core fetch: 1419 samples, 142 datasets
    259,    // measured values (method group 2)
    3581,   // the only site with analysis entities carrying two relative dates
    3718,   // ceramics (method 171), 480 single-AE datasets
    4149,   // dendrochronology (method 10), 690 fragmented datasets
    4355,   // worst case for AbundanceModule: 5573 abundance rows
    4635,   // C14 std dating: 57 geochronology rows across 3 dating labs
    5587,   // large abundance site
    5130,   // 20 site_references: exercises the unawaited biblio lookup hardest
    5615,   // large abundance site
    6486,   // aDNA (method 175)
    999999, // edge case: nonexistent site id
];

export function parseArgs(argv) {
    const args = { base: "http://localhost:8485", timeout: 600000, method: "false" };
    for(let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        if(arg == "--out") args.out = argv[++i];
        else if(arg == "--base") args.base = argv[++i];
        else if(arg == "--sites") args.sites = argv[++i].split(",").map(s => parseInt(s.trim()));
        else if(arg == "--method") args.method = argv[++i];
        else if(arg == "--timeout") args.timeout = parseInt(argv[++i]);
        else throw new Error("Unknown argument: " + arg);
    }
    if(!args.out) throw new Error("--out <dir> is required");
    if(!args.sites) args.sites = DEFAULT_SITES;
    return args;
}

/** Reads the server's query counter, if the instrumentation is enabled. */
export async function readQueryStats(base) {
    try {
        const response = await fetch(base + "/debug/query-stats");
        if(!response.ok) return null;
        return await response.json();
    }
    catch(error) {
        return null;
    }
}

/**
 * Builds the site URL. The third path segment selects the fetch implementation:
 * "true" is the original per-row getSite(); anything else (including "false") is
 * the default consolidated getSitePostgres().
 */
export function siteUrl(base, siteId, fetchMethod) {
    // /site/:siteId/:noCache?/:alternativeFetchMethod?
    return base + "/site/" + siteId + "/true/" + (fetchMethod || "false");
}

export async function captureSite(base, siteId, options = {}) {
    const url = siteUrl(base, siteId, options.method);
    const before = await readQueryStats(base);
    const started = Date.now();
    const response = await fetch(url, { signal: AbortSignal.timeout(options.timeout || 600000) });
    const text = await response.text();
    const elapsedMs = Date.now() - started;
    const after = await readQueryStats(base);

    let payload;
    try {
        payload = JSON.parse(text);
    }
    catch(error) {
        throw new Error("Site " + siteId + " returned non-JSON (" + response.status + "): " + text.slice(0, 200));
    }

    const queries = (before && after) ? after.count - before.count : null;
    return { siteId, payload, elapsedMs, queries, status: response.status, bytes: Buffer.byteLength(text) };
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    fs.mkdirSync(args.out, { recursive: true });

    const stats = [];
    console.log("Capturing " + args.sites.length + " site(s) from " + args.base +
        " [fetch method: " + args.method + "]");

    for(const siteId of args.sites) {
        process.stdout.write("  site " + siteId + " … ");
        try {
            const result = await captureSite(args.base, siteId, args);
            fs.writeFileSync(
                path.join(args.out, siteId + ".json"),
                JSON.stringify(result.payload, null, 2)
            );
            stats.push({
                site_id: siteId,
                ms: result.elapsedMs,
                queries: result.queries,
                bytes: result.bytes,
                status: result.status,
            });
            console.log(result.elapsedMs + "ms  " +
                (result.queries === null ? "queries=n/a" : "queries=" + result.queries) + "  " +
                (result.bytes / 1024 / 1024).toFixed(2) + "MB");
        }
        catch(error) {
            console.log("FAILED: " + error.message);
            stats.push({ site_id: siteId, error: error.message });
        }
    }

    fs.writeFileSync(path.join(args.out, "_stats.json"), JSON.stringify(stats, null, 2));
    console.log("Wrote " + stats.length + " file(s) to " + args.out);
}

if(import.meta.url == "file://" + process.argv[1]) {
    main().catch(error => { console.error(error); process.exit(1); });
}
