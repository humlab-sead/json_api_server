/**
 * User roles (src/Lib/Auth/UserRoles.js): what a user_roles document grants.
 * Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import UserRoles, { rolesFromDocument } from "../../src/Lib/Auth/UserRoles.js";

test("a document grants its known roles, once each", () => {
    assert.deepEqual(rolesFromDocument({ _id: "orcid:0000-0002-1825-0097", roles: ["sysadmin", "sysadmin"] }), ["sysadmin"]);
});

test("unknown roles and non-strings grant nothing", () => {
    assert.deepEqual(rolesFromDocument({ roles: ["sysadmn", "admin", 1, null] }), []);
    assert.deepEqual(rolesFromDocument({ roles: ["toString", "__proto__"] }), []);
});

test("no document, or one without a roles array, grants nothing", () => {
    assert.deepEqual(rolesFromDocument(null), []);
    assert.deepEqual(rolesFromDocument({ roles: "sysadmin" }), []);
});

test("a signed-out request has no roles, without asking Mongo", async () => {
    const roles = new UserRoles({ get mongoReady() { throw new Error("Mongo was asked"); } });
    assert.deepEqual(await roles.rolesOf(null), []);
});

test("roles are read from user_roles by user id", async () => {
    const asked = [];
    const app = {
        mongoReady: Promise.resolve(),
        mongo: {
            collection: name => ({
                findOne: async query => {
                    asked.push([name, query]);
                    return { _id: query._id, roles: ["sysadmin"] };
                },
            }),
        },
    };
    assert.deepEqual(await new UserRoles(app).rolesOf("saml:abc@umu.se"), ["sysadmin"]);
    assert.deepEqual(asked, [["user_roles", { _id: "saml:abc@umu.se" }]]);
});
