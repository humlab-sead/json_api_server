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

/**
 * Identity keys, most specific first. Aligning arrays of records by identity
 * rather than by content is what keeps a diff readable: if one side carries an
 * extra column, content-based alignment reshuffles both arrays and reports
 * every neighbouring field as changed, burying the one real difference.
 */
const IDENTITY_KEYS = [
    "analysis_entity_id",
    "physical_sample_id",
    "sample_group_id",
    "dataset_id",
    "biblio_id",
    "taxon_id",
    "method_id",
    "dimension_id",
    "unit_id",
    "site_id",
    "location_id",
    "contact_id",
    "project_id",
    "horizon_id",
    "feature_id",
    "value_class_id",
];

function isPrimitive(value) {
    return value === null || (typeof value != "object");
}

/**
 * Picks a key usable as an identity for aligning two arrays of objects: it must
 * be present with a primitive value on every element of both arrays, and be
 * unique within each array.
 */
function findIdentityKey(left, right) {
    const objectsOnly = array => array.every(element =>
        element !== null && typeof element == "object" && !Array.isArray(element));
    if(left.length == 0 || right.length == 0 || !objectsOnly(left) || !objectsOnly(right)) {
        return null;
    }

    const candidates = IDENTITY_KEYS.filter(key =>
        Object.prototype.hasOwnProperty.call(left[0], key) &&
        Object.prototype.hasOwnProperty.call(right[0], key));

    for(const key of candidates) {
        const usable = [left, right].every(array => {
            const values = new Set();
            for(const element of array) {
                const value = element[key];
                if(!Object.prototype.hasOwnProperty.call(element, key) || !isPrimitive(value) || value === null) {
                    return false;
                }
                // Compare loosely so that the string "1" and the number 1 align,
                // letting the type difference itself be reported on the field.
                const normalised = String(value);
                if(values.has(normalised)) {
                    return false;
                }
                values.add(normalised);
            }
            return true;
        });
        if(usable) {
            return key;
        }
    }
    return null;
}

function typeOf(value) {
    if(value === null) return "null";
    if(Array.isArray(value)) return "array";
    return typeof value;
}

/** Collapses concrete array indices so differences can be grouped by shape. */
function patternOf(path) {
    return path.replace(/\[[^\]]*\]/g, "[*]");
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
        return text.length > 120 ? text.slice(0, 120) + "…" : text;
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

            const identityKey = sortArrays ? findIdentityKey(expectedValue, actualValue) : null;
            if(identityKey) {
                const rightByKey = new Map(actualValue.map(element => [String(element[identityKey]), element]));
                const matched = new Set();
                for(const leftElement of expectedValue) {
                    const keyValue = String(leftElement[identityKey]);
                    const rightElement = rightByKey.get(keyValue);
                    if(rightElement === undefined) {
                        record("missing-element", path + "[" + identityKey + "=" + keyValue + "]", {
                            expected: truncate(leftElement),
                        });
                        continue;
                    }
                    matched.add(keyValue);
                    walk(leftElement, rightElement, path + "[" + identityKey + "=" + keyValue + "]");
                }
                for(const rightElement of actualValue) {
                    const keyValue = String(rightElement[identityKey]);
                    if(!matched.has(keyValue)) {
                        record("extra-element", path + "[" + identityKey + "=" + keyValue + "]", {
                            actual: truncate(rightElement),
                        });
                    }
                }
                return;
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
