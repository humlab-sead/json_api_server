/**
 * The admin panel's user administration (src/EndpointModules/UserAdmin.class.js), and the
 * session and user records it reads. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import AuthenticationHandler from "../../src/AuthenticationHandler.class.js";
import UserAdmin, { userEntry, parseRolesBody, parseRoleBody, keepsAdmin } from "../../src/EndpointModules/UserAdmin.class.js";
import { roleDefinitions } from "../../src/Lib/Auth/UserRoles.js";
import { isUserId } from "../../src/Lib/Auth/AuthIdentity.js";
import { liveSessionsByUser, endSessionsOf, sessionUser } from "../../src/Lib/Auth/Sessions.js";
import { profileOf } from "../../src/Lib/Auth/UserDirectory.js";

const ORIGIN = "https://sead.local";
const ADMIN = { provider: "saml", id: "admin@umu.se", displayName: "Ada Admin", emails: [{ value: "ada@umu.se" }], organization: "umu.se" };
const READER = { provider: "orcid", id: "0000-0002-1825-0097", displayName: "Rita Reader", uri: "https://orcid.org/0000-0002-1825-0097", emails: [] };

function response() {
    return {
        statusCode: 200, body: null, headers: {},
        status(code) { this.statusCode = code; return this; },
        json(body) { this.body = body; return this; },
        set(name, value) { this.headers[name] = value; },
        setHeader(name, value) { this.headers[name] = value; },
    };
}

function request(user, { method = "GET", headers = {}, params = {}, body } = {}) {
    return {
        method, path: "/admin/users", user, params, body,
        isAuthenticated: () => user != null,
        get: name => headers[name.toLowerCase()],
    };
}

test("user ids: the forms logins give, and nothing else", () => {
    for (const id of ["saml:alice@umu.se", "orcid:0000-0002-1825-0097", "bob@gmail.com-google", "bob@x.se-github"]) {
        assert.equal(isUserId(id), true, id);
    }
    for (const id of ["alice@umu.se", "saml:", "orcid: x", "-google", "", null, 42, "saml:"+"x".repeat(600)]) {
        assert.equal(isUserId(id), false, String(id));
    }
});

test("a permission-gated GET needs no Origin; a PUT needs ours", async () => {
    const h = Object.create(AuthenticationHandler.prototype);
    h.publicOrigin = ORIGIN;
    h.userDirectory = { hasConsented: async () => true };
    h.userRoles = { accessOf: async id => id === "saml:admin@umu.se" ? { roles: ["sysadmin"], permissions: ["administer_users"] } : { roles: [], permissions: [] } };
    const gate = h.requirePermission("administer_users");
    const pass = async req => {
        const res = response();
        let passed = false;
        await gate(req, res, () => { passed = true; });
        return { passed, res };
    };

    assert.equal((await pass(request(ADMIN))).passed, true);
    assert.equal((await pass(request(ADMIN, { method: "PUT", headers: { origin: ORIGIN } }))).passed, true);
    const crossOrigin = await pass(request(ADMIN, { method: "PUT", headers: { origin: "https://evil.example" } }));
    assert.equal(crossOrigin.passed, false);
    assert.equal(crossOrigin.res.statusCode, 403);
    const reader = await pass(request(READER));
    assert.equal(reader.passed, false);
    assert.equal(reader.res.body.code, "forbidden");
});

test("the protected-endpoint password does not open requirePermission", async () => {
    const h = Object.create(AuthenticationHandler.prototype);
    h.app = { checkBasicAuth: () => assert.fail("basic auth was consulted") };
    h.userRoles = { accessOf: async () => ({ roles: [], permissions: [] }) };
    const res = response();
    await h.requirePermission("administer_users")(request(null, { headers: { authorization: "Basic eDp5" } }), res, () => assert.fail("passed"));
    assert.equal(res.statusCode, 401);
});

const DEFINITIONS = roleDefinitions([{ _id: "researcher", description: "", permissions: ["sead_agent"] }]);

test("a user PUT body: roles that exist, once each, and an optional note", () => {
    const parse = body => parseRolesBody(body, DEFINITIONS);
    assert.deepEqual(parse({ roles: ["sysadmin", "sysadmin", "researcher"] }), { roles: ["sysadmin", "researcher"], note: undefined });
    assert.deepEqual(parse({ roles: [], note: null }), { roles: [], note: "" });
    assert.match(parse({ roles: ["root"] }).error, /no role "root"/);
    assert.match(parse({ roles: ["__proto__"] }).error, /no role/);
    assert.match(parse({ roles: "sysadmin" }).error, /array/);
    assert.match(parse({ roles: [], note: 3 }).error, /note/);
    assert.match(parse({ roles: [], note: "x".repeat(501) }).error, /longer/);
});

test("a role body: known permissions, once each, and a description", () => {
    assert.deepEqual(parseRoleBody({ permissions: ["sead_agent", "sead_agent"], description: " Uses the agent " }), { permissions: ["sead_agent"], description: "Uses the agent" });
    assert.deepEqual(parseRoleBody({ permissions: [] }), { permissions: [], description: "" });
    assert.match(parseRoleBody({ permissions: ["root"] }).error, /no permission "root"/);
    assert.match(parseRoleBody({ permissions: "sead_agent" }).error, /array/);
    assert.match(parseRoleBody({ permissions: [], description: "x".repeat(301) }).error, /longer/);
});

test("keepsAdmin: whether roles, as defined, still administer users", () => {
    assert.equal(keepsAdmin(["sysadmin"], DEFINITIONS), true);
    assert.equal(keepsAdmin(["researcher"], DEFINITIONS), false);
    assert.equal(keepsAdmin([], DEFINITIONS), false);
});

test("a user entry joins what the directory, user_roles and sessions know", () => {
    const signedInAt = new Date("2026-10-07T08:00:00Z");
    const entry = userEntry("saml:admin@umu.se", {
        directory: { ...profileOf(ADMIN), first_sign_in_at: signedInAt, last_sign_in_at: signedInAt, sign_ins: 3 },
        roleDoc: { roles: ["sysadmin", "gone"], note: "data manager", updated_by: "saml:root@umu.se" },
        session: { user: ADMIN, sessionIds: ["s1"] },
    }, DEFINITIONS);
    assert.equal(entry.display_name, "Ada Admin");
    assert.equal(entry.email, "ada@umu.se");
    assert.equal(entry.signed_in, true);
    assert.deepEqual(entry.roles, ["sysadmin"]);
    assert.equal(entry.note, "data manager");
    assert.equal(entry.sign_ins, 3);

    //given a role ahead of a first sign-in: only the id is known
    const ahead = userEntry("bob@gmail.com-google", { roleDoc: { roles: ["sysadmin"] } }, DEFINITIONS);
    assert.equal(ahead.provider, "google");
    assert.equal(ahead.display_name, null);
    assert.equal(ahead.signed_in, false);

    //signed in before sign-ins were recorded: the session has the profile
    const fromSession = userEntry("orcid:0000-0002-1825-0097", { session: { user: READER, sessionIds: ["s2"] } }, DEFINITIONS);
    assert.equal(fromSession.display_name, "Rita Reader");
    assert.equal(fromSession.uri, READER.uri);
});

/** A sessions collection as connect-mongo leaves it: the session as a JSON string. */
function sessionsDb(docs) {
    const deleted = [];
    return {
        deleted,
        collection: () => ({
            find: query => ({ toArray: async () => docs.filter(doc => doc.expires > query.expires.$gt) }),
            deleteMany: async query => {
                deleted.push(...query._id.$in);
                return { deletedCount: query._id.$in.length };
            },
        }),
    };
}

const later = new Date(Date.now() + 3600 * 1000);
const SESSIONS = [
    { _id: "s1", expires: later, session: JSON.stringify({ passport: { user: ADMIN } }) },
    { _id: "s2", expires: later, session: JSON.stringify({ passport: { user: READER } }) },
    { _id: "s3", expires: later, session: JSON.stringify({ passport: { user: READER } }) },
    { _id: "s4", expires: new Date(0), session: JSON.stringify({ passport: { user: ADMIN } }) },
    { _id: "s5", expires: later, session: JSON.stringify({ cookie: {} }) },
    { _id: "s6", expires: later, session: "{not json" },
];

test("live sessions are grouped by user; expired, anonymous and broken ones are left out", async () => {
    assert.equal(sessionUser(SESSIONS[5]), null);
    const live = await liveSessionsByUser(sessionsDb(SESSIONS));
    assert.deepEqual([...live.keys()].sort(), ["orcid:0000-0002-1825-0097", "saml:admin@umu.se"]);
    assert.deepEqual(live.get("orcid:0000-0002-1825-0097").sessionIds, ["s2", "s3"]);
});

test("ending a user's sessions deletes theirs only", async () => {
    const db = sessionsDb(SESSIONS);
    assert.equal(await endSessionsOf(db, "orcid:0000-0002-1825-0097"), 2);
    assert.deepEqual(db.deleted, ["s2", "s3"]);
    assert.equal(await endSessionsOf(db, "saml:nobody@umu.se"), 0);
});

/** UserAdmin on a fake app, with user roles and role documents kept in Maps. */
function userAdmin(roles = new Map([["saml:admin@umu.se", ["sysadmin"]]]), roleDocs = new Map([["researcher", { _id: "researcher", description: "", permissions: ["sead_agent"] }]])) {
    const routes = {};
    const route = method => (path, gate, handler) => { routes[method+" "+path] = handler; };
    const auth = {
        requirePermission: () => () => {},
        getUserId: req => req.user ? `${req.user.provider}:${req.user.id}` : null,
        userRoles: {
            definitions: async () => roleDefinitions([...roleDocs.values()]),
            rolesOf: async id => roles.get(id) || [],
            setRoles: async (id, newRoles) => { roles.set(id, newRoles); return newRoles; },
            saveRole: async (id, { description, permissions }) => {
                roleDocs.set(id, { _id: id, description, permissions });
                return roleDefinitions([...roleDocs.values()]).get(id);
            },
            deleteRole: async id => {
                roleDocs.delete(id);
                let users = 0;
                for (const [user, userRoles] of roles) {
                    if (userRoles.includes(id)) { roles.set(user, userRoles.filter(r => r !== id)); users++; }
                }
                return users;
            },
        },
    };
    new UserAdmin({ authHandler: auth, expressApp: { get: route("GET"), put: route("PUT"), post: route("POST"), delete: route("DELETE") } });
    const call = async (method, path, params, body) => {
        const res = response();
        await routes[method+" "+path](request(ADMIN, { method, params, body }), res);
        return res;
    };
    return { routes, roles, roleDocs, call };
}

test("an admin cannot take away their own permission to administer users", async () => {
    const { call, roles } = userAdmin();
    const res = await call("PUT", "/admin/users/:userId", { userId: "saml:admin@umu.se" }, { roles: ["researcher"] });
    assert.equal(res.statusCode, 409);
    assert.equal(res.body.code, "own_admin_permission");
    assert.deepEqual(roles.get("saml:admin@umu.se"), ["sysadmin"]);
});

test("an admin can give and take another user's roles, by id", async () => {
    const { call, roles } = userAdmin();
    const put = (userId, body) => call("PUT", "/admin/users/:userId", { userId }, body);
    assert.deepEqual((await put("orcid:0000-0002-1825-0097", { roles: ["researcher"] })).body.roles, ["researcher"]);
    assert.deepEqual(roles.get("orcid:0000-0002-1825-0097"), ["researcher"]);
    assert.deepEqual((await put("orcid:0000-0002-1825-0097", { roles: [] })).body.roles, []);
    assert.equal((await put("not-an-id", { roles: [] })).statusCode, 400);
    assert.equal((await put("orcid:0000-0002-1825-0097", { roles: ["root"] })).statusCode, 400);
});

test("roles are created, changed and deleted; deleting one takes it from its users", async () => {
    const { call, roles, roleDocs } = userAdmin(new Map([["saml:admin@umu.se", ["sysadmin"]], ["orcid:0000-0002-1825-0097", ["agent-users"]]]));
    const created = await call("POST", "/admin/roles", {}, { id: "agent-users", description: "May chat", permissions: ["sead_agent"] });
    assert.equal(created.statusCode, 201);
    assert.deepEqual(created.body.permissions, ["sead_agent"]);
    assert.equal((await call("POST", "/admin/roles", {}, { id: "agent-users", permissions: [] })).statusCode, 409);
    assert.equal((await call("POST", "/admin/roles", {}, { id: "Bad Name", permissions: [] })).statusCode, 400);
    assert.equal((await call("POST", "/admin/roles", {}, { id: "sysadmin", permissions: [] })).statusCode, 409);

    const changed = await call("PUT", "/admin/roles/:roleId", { roleId: "agent-users" }, { description: "", permissions: [] });
    assert.deepEqual(changed.body.permissions, []);
    assert.equal((await call("PUT", "/admin/roles/:roleId", { roleId: "nope" }, { permissions: [] })).statusCode, 404);

    const deleted = await call("DELETE", "/admin/roles/:roleId", { roleId: "agent-users" });
    assert.equal(deleted.body.users, 1);
    assert.equal(roleDocs.has("agent-users"), false);
    assert.deepEqual(roles.get("orcid:0000-0002-1825-0097"), []);
});

test("sysadmin cannot be deleted, nor lose Administer users", async () => {
    const { call } = userAdmin();
    assert.equal((await call("DELETE", "/admin/roles/:roleId", { roleId: "sysadmin" })).body.code, "builtin_role");
    const res = await call("PUT", "/admin/roles/:roleId", { roleId: "sysadmin" }, { permissions: ["sead_agent"] });
    assert.equal(res.body.code, "locked_permission");
    //but its other permissions are the admins' to decide
    assert.deepEqual((await call("PUT", "/admin/roles/:roleId", { roleId: "sysadmin" }, { permissions: ["administer_users"] })).body.permissions, ["administer_users"]);
});

test("an admin cannot change or delete the role their own admin permission comes from", async () => {
    const { call } = userAdmin(new Map([["saml:admin@umu.se", ["useradmins"]]]),
        new Map([["useradmins", { _id: "useradmins", description: "", permissions: ["administer_users"] }]]));
    assert.equal((await call("PUT", "/admin/roles/:roleId", { roleId: "useradmins" }, { permissions: ["sead_agent"] })).body.code, "own_admin_permission");
    assert.equal((await call("DELETE", "/admin/roles/:roleId", { roleId: "useradmins" })).body.code, "own_admin_permission");
});
