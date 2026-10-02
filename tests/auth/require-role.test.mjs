/**
 * The gate on endpoints that need a role (AuthenticationHandler.requireRoleOrBasicAuth):
 * a signed-in user with the role, from our own origin, or the protected-endpoint password.
 * Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import AuthenticationHandler from "../../src/AuthenticationHandler.class.js";

const ORIGIN = "https://sead.local";
const SYSADMIN = { provider: "orcid", id: "0000-0002-1825-0097" };
const READER = { provider: "saml", id: "reader@umu.se" };

/** A handler with only what the gate uses: the roles store, the user and the origin. */
function handler(roles = { "orcid:0000-0002-1825-0097": ["sysadmin"] }, { failRoles = false } = {}) {
    const h = Object.create(AuthenticationHandler.prototype);
    h.publicOrigin = ORIGIN;
    h.basicAuthCalls = 0;
    h.app = { checkBasicAuth: (req, res, next) => { h.basicAuthCalls++; next(); } };
    h.userRoles = {
        rolesOf: async id => {
            if (failRoles) throw new Error("mongo down");
            return roles[id] || [];
        },
    };
    return h;
}

function request(user, headers = { origin: ORIGIN }) {
    return {
        method: "POST",
        path: "/sdf/validate",
        user,
        isAuthenticated: () => user != null,
        get: name => headers[name.toLowerCase()],
    };
}

async function run(h, req) {
    const res = {
        statusCode: 200, body: null, headers: {},
        status(code) { this.statusCode = code; return this; },
        json(body) { this.body = body; return this; },
        setHeader(name, value) { this.headers[name] = value; },
    };
    let passed = false;
    await h.requireRoleOrBasicAuth("sysadmin")(req, res, () => { passed = true; });
    return { passed, res };
}

test("a sysadmin from our origin passes", async () => {
    const { passed } = await run(handler(), request(SYSADMIN));
    assert.equal(passed, true);
});

test("a signed-in user without the role is refused with 403", async () => {
    const { passed, res } = await run(handler(), request(READER));
    assert.equal(passed, false);
    assert.equal(res.statusCode, 403);
    assert.equal(res.body.code, "forbidden");
});

test("a sysadmin's session from another origin is refused", async () => {
    const { passed, res } = await run(handler(), request(SYSADMIN, { origin: "https://evil.example" }));
    assert.equal(passed, false);
    assert.equal(res.statusCode, 403);
});

test("signed out, without a password: 401 and no browser password prompt", async () => {
    const { passed, res } = await run(handler(), request(null));
    assert.equal(passed, false);
    assert.equal(res.statusCode, 401);
    assert.equal(res.body.code, "sign_in_required");
    assert.equal(res.headers["WWW-Authenticate"], undefined);
});

test("a request with an Authorization header is judged on the password alone", async () => {
    const h = handler();
    const { passed } = await run(h, request(READER, { authorization: "Basic eDp5" }));
    assert.equal(passed, true);
    assert.equal(h.basicAuthCalls, 1);
});

test("roles that cannot be read refuse with 503 rather than let anyone through", async () => {
    const { passed, res } = await run(handler(undefined, { failRoles: true }), request(SYSADMIN));
    assert.equal(passed, false);
    assert.equal(res.statusCode, 503);
});
