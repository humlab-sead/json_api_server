/**
 * Accepting the privacy policy (AuthenticationHandler.class.js, UserDirectory.js): nothing is
 * kept and no role counts until a signed-in user has accepted the current version, the first
 * acceptance gives them the default role, and they can delete their account. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import AuthenticationHandler from "../../src/AuthenticationHandler.class.js";
import UserDirectory, { PRIVACY_POLICY_VERSION, hasCurrentConsent } from "../../src/Lib/Auth/UserDirectory.js";
import { roleDefinitions } from "../../src/Lib/Auth/UserRoles.js";

const USER = { provider: "orcid", id: "0000-0002-1825-0097", displayName: "Rita Reader", uri: "https://orcid.org/0000-0002-1825-0097", emails: [] };
const USER_ID = "orcid:0000-0002-1825-0097";

/** A users collection over a Map, with what UserDirectory uses. */
function usersCollection(docs = new Map()) {
    return {
        docs,
        findOne: async query => docs.get(query._id) || null,
        updateOne: async (query, update) => {
            const doc = docs.get(query._id);
            if (doc && (!query.privacy_consent || doc.privacy_consent)) Object.assign(doc, update.$set);
        },
        findOneAndUpdate: async (query, update) => {
            const before = docs.get(query._id);
            const copy = before ? structuredClone(before) : null;
            docs.set(query._id, { _id: query._id, ...(before ? {} : update.$setOnInsert), ...before, ...update.$set });
            return copy;
        },
        deleteOne: async query => docs.delete(query._id),
    };
}

function directory(docs) {
    const users = usersCollection(docs);
    return { users, directory: new UserDirectory({ mongoReady: Promise.resolve(), mongo: { collection: () => users } }) };
}

test("only an acceptance of the current version counts", () => {
    assert.equal(hasCurrentConsent({ privacy_consent: { version: PRIVACY_POLICY_VERSION } }), true);
    assert.equal(hasCurrentConsent({ privacy_consent: { version: "2020-01-01" } }), false);
    assert.equal(hasCurrentConsent({ display_name: "x" }), false);
    assert.equal(hasCurrentConsent(null), false);
});

test("a sign-in without an account writes nothing", async () => {
    const { users, directory: dir } = directory();
    await dir.recordSignIn(USER);
    assert.equal(users.docs.size, 0);
});

test("accepting creates the account; only the first acceptance is the first", async () => {
    const { users, directory: dir } = directory();
    assert.equal(await dir.recordConsent(USER, PRIVACY_POLICY_VERSION), true);
    const doc = users.docs.get(USER_ID);
    assert.equal(doc.display_name, "Rita Reader");
    assert.equal(doc.privacy_consent.version, PRIVACY_POLICY_VERSION);
    assert.equal(await dir.hasConsented(USER_ID), true);
    assert.equal(await dir.recordConsent(USER, PRIVACY_POLICY_VERSION), false);
});

/** An AuthenticationHandler on fakes: the directory over a Map, roles in a Map. */
function handler({ consented = false, roles = [] } = {}) {
    const h = Object.create(AuthenticationHandler.prototype);
    const { users, directory: dir } = directory(consented
        ? new Map([[USER_ID, { _id: USER_ID, privacy_consent: { version: PRIVACY_POLICY_VERSION } }]]) : new Map());
    h.userDirectory = dir;
    const userRoles = new Map(roles.length ? [[USER_ID, roles]] : []);
    h.userRoles = {
        addRole: async (id, role) => userRoles.set(id, [...(userRoles.get(id) || []), role]),
        accessOf: async id => {
            const definitions = roleDefinitions();
            const has = userRoles.get(id) || [];
            return { roles: has, permissions: [...new Set(has.flatMap(r => definitions.get(r).permissions))] };
        },
    };
    h.deleted = [];
    h.deleteAccount = async id => { h.deleted.push(id); userRoles.delete(id); users.docs.delete(id); };
    return { h, users, userRoles };
}

function request(user, { method = "POST", body } = {}) {
    return {
        method, path: "/auth/consent", user, body,
        isAuthenticated: () => user != null,
        get: () => undefined,
        logout: callback => callback(),
        session: { destroy: callback => callback() },
    };
}

function response() {
    return {
        statusCode: 200, body: null, cleared: null,
        status(code) { this.statusCode = code; return this; },
        json(body) { this.body = body; return this; },
        set() {},
        clearCookie(name) { this.cleared = name; },
    };
}

test("until the policy is accepted, roles count for nothing", async () => {
    const { h } = handler({ roles: ["sysadmin"] });
    const res = response();
    let passed = false;
    await h.requirePermission("administer_users")(request(USER, { method: "GET" }), res, () => { passed = true; });
    assert.equal(passed, false);
    assert.equal(res.body.code, "consent_required");
    const access = await h.getAccessOrNone(request(USER));
    assert.deepEqual(access.roles, []);
    assert.deepEqual(access.consent, { required: true, version: PRIVACY_POLICY_VERSION });
});

test("signed out, no consent is asked for", async () => {
    const { h } = handler();
    assert.equal((await h.getAccessOrNone(request(null))).consent.required, false);
});

test("viewstates need the policy accepted", async () => {
    const refused = response();
    await handler().h.requireConsent(request(USER), refused, () => assert.fail("passed"));
    assert.equal(refused.body.code, "consent_required");
    let passed = false;
    await handler({ consented: true }).h.requireConsent(request(USER), response(), () => { passed = true; });
    assert.equal(passed, true);
});

test("accepting the current policy gives the default role, once; another version is refused", async () => {
    const { h, userRoles } = handler();
    const stale = response();
    await h.handleConsent(request(USER, { body: { version: "2020-01-01" } }), stale);
    assert.equal(stale.statusCode, 409);
    assert.equal(stale.body.code, "policy_changed");
    assert.equal(userRoles.size, 0);

    const res = response();
    await h.handleConsent(request(USER, { body: { version: PRIVACY_POLICY_VERSION } }), res);
    assert.equal(res.statusCode, 200);
    assert.equal(res.body.consent.required, false);
    assert.deepEqual(res.body.roles, ["user"]);

    await h.handleConsent(request(USER, { body: { version: PRIVACY_POLICY_VERSION } }), response());
    assert.deepEqual(userRoles.get(USER_ID), ["user"]);
});

test("a user can delete their account; an admin cannot", async () => {
    const { h } = handler({ consented: true, roles: ["user"] });
    const res = response();
    await h.handleDeleteAccount(request(USER, { method: "DELETE" }), res);
    assert.deepEqual(res.body, { deleted: true });
    assert.deepEqual(h.deleted, [USER_ID]);
    assert.equal(res.cleared, "__Host-sead.sid");

    const admin = handler({ consented: true, roles: ["sysadmin"] });
    const refused = response();
    await admin.h.handleDeleteAccount(request(USER, { method: "DELETE" }), refused);
    assert.equal(refused.body.code, "admin_account");
    assert.deepEqual(admin.h.deleted, []);
});
