/**
 * Semantic diffing helpers for site JSON payloads.
 *
 * The site payload is a deep tree of arrays whose ordering is not specified by
 * the current implementation (most queries have no ORDER BY), so a textual diff
 * produces thousands of false positives. These helpers compare structurally and
 * are deliberately type-sensitive: the pg driver returns `numeric` columns as
 * strings, so "12.5" and 12.5 must never compare equal.
 */

/**
 * Stable serialisation used both for comparison and for sorting array elements.
 * Object keys are sorted so that key order never affects the result. Strings and
 * numbers serialise differently, which is what keeps the comparison type-sensitive.
 */
export function canonicalize(value) {
    if(value === null || typeof value != "object") {
        return JSON.stringify(value === undefined ? null : value);
    }
    if(Array.isArray(value)) {
        return "[" + value.map(canonicalize).join(",") + "]";
    }
    const keys = Object.keys(value).sort();
    return "{" + keys.map(key => JSON.stringify(key) + ":" + canonicalize(value[key])).join(",") + "}";
}

/** Sorts a copy of an array by the canonical form of its elements. */
function sortedCopy(array) {
    return array
        .map(element => ({ element, key: canonicalize(element) }))
        .sort((a, b) => a.key < b.key ? -1 : (a.key > b.key ? 1 : 0))
        .map(entry => entry.element);
}

function typeOf(value) {
    if(value === null) return "null";
    if(Array.isArray(value)) return "array";
    return typeof value;
}

/** Collapses concrete array indices so differences can be grouped by shape. */
function patternOf(path) {
    return path.replace(/\[\d+\]/g, "[*]");
}

/**
 * Walks two payloads and records every difference.
 *
 * @param {object} options.sortArrays  compare arrays as multisets (ignores ordering)
 * @param {number} options.maxSamples  how many concrete examples to keep per pattern
 */
export function diffValues(expected, actual, options = {}) {
    const sortArrays = options.sortArrays !== false;
    const maxSamples = options.maxSamples || 3;
    const groups = new Map();

    function record(kind, path, detail) {
        const pattern = patternOf(path);
        const groupKey = kind + " @ " + pattern;
        let group = groups.get(groupKey);
        if(!group) {
            group = { kind, pattern, count: 0, samples: [] };
            groups.set(groupKey, group);
        }
        group.count++;
        if(group.samples.length < maxSamples) {
            group.samples.push({ path, ...detail });
        }
    }

    function truncate(value) {
        const text = canonicalize(value);
        return text.length > 200 ? text.slice(0, 200) + "…" : text;
    }

    function walk(expectedValue, actualValue, path) {
        const expectedType = typeOf(expectedValue);
        const actualType = typeOf(actualValue);

        if(expectedType != actualType) {
            record("type", path, {
                expected: expectedType + " " + truncate(expectedValue),
                actual: actualType + " " + truncate(actualValue),
            });
            return;
        }

        if(expectedType == "array") {
            if(expectedValue.length != actualValue.length) {
                record("array-length", path, {
                    expected: expectedValue.length,
                    actual: actualValue.length,
                });
            }
            const left = sortArrays ? sortedCopy(expectedValue) : expectedValue;
            const right = sortArrays ? sortedCopy(actualValue) : actualValue;
            const shared = Math.min(left.length, right.length);
            for(let index = 0; index < shared; index++) {
                walk(left[index], right[index], path + "[" + index + "]");
            }
            return;
        }

        if(expectedType == "object") {
            const expectedKeys = Object.keys(expectedValue);
            const actualKeys = Object.keys(actualValue);
            for(const key of expectedKeys) {
                if(!Object.prototype.hasOwnProperty.call(actualValue, key)) {
                    record("missing-key", path + "." + key, { expected: truncate(expectedValue[key]) });
                }
            }
            for(const key of actualKeys) {
                if(!Object.prototype.hasOwnProperty.call(expectedValue, key)) {
                    record("extra-key", path + "." + key, { actual: truncate(actualValue[key]) });
                }
            }
            for(const key of expectedKeys) {
                if(Object.prototype.hasOwnProperty.call(actualValue, key)) {
                    walk(expectedValue[key], actualValue[key], path + "." + key);
                }
            }
            return;
        }

        if(expectedValue !== actualValue) {
            record("value", path, { expected: truncate(expectedValue), actual: truncate(actualValue) });
        }
    }

    walk(expected, actual, "$");
    return Array.from(groups.values()).sort((a, b) => b.count - a.count);
}

/** True when the two payloads are identical apart from array ordering. */
export function isEquivalent(expected, actual) {
    return diffValues(expected, actual).length == 0;
}

export function formatDiff(groups, options = {}) {
    const maxGroups = options.maxGroups || 40;
    if(groups.length == 0) {
        return "  (no differences)";
    }
    const lines = [];
    const total = groups.reduce((sum, group) => sum + group.count, 0);
    lines.push("  " + total + " difference(s) in " + groups.length + " distinct path pattern(s)");
    groups.slice(0, maxGroups).forEach(group => {
        lines.push("  [" + group.kind + "] " + group.pattern + "  x" + group.count);
        group.samples.forEach(sample => {
            const parts = [];
            if(sample.expected !== undefined) parts.push("expected=" + sample.expected);
            if(sample.actual !== undefined) parts.push("actual=" + sample.actual);
            lines.push("      " + sample.path + "  " + parts.join("  "));
        });
    });
    if(groups.length > maxGroups) {
        lines.push("  … and " + (groups.length - maxGroups) + " more pattern(s)");
    }
    return lines.join("\n");
}
