/**
 * User roles (src/Lib/Auth/UserRoles.js): the roles there are, what a user_roles document
 * grants, and the permissions roles give. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import UserRoles, { rolesFromDocument, roleDefinitions, permissionsOfRoles } from "../../src/Lib/Auth/UserRoles.js";

test("sysadmin exists without a document, and administers users", () => {
    const sysadmin = roleDefinitions().get("sysadmin");
    assert.equal(sysadmin.builtin, true);
    assert.deepEqual(sysadmin.permissions, ["administer_users", "sead_agent"]);
});

test("a stored role is defined by its document; unknown permissions and bad ids are dropped", () => {
    const definitions = roleDefinitions([
        { _id: "researcher", description: "Uses the agent", permissions: ["sead_agent", "root", "sead_agent"] },
        { _id: "Bad Id", permissions: ["sead_agent"] },
    ]);
    assert.deepEqual(definitions.get("researcher").permissions, ["sead_agent"]);
    assert.equal(definitions.get("researcher").builtin, false);
    assert.equal(definitions.has("Bad Id"), false);
});

test("a built-in role keeps its locked permissions whatever its document says", () => {
    const sysadmin = roleDefinitions([{ _id: "sysadmin", permissions: [] }]).get("sysadmin");
    assert.deepEqual(sysadmin.permissions, ["administer_users"]);
});

test("a document grants the roles that exist, once each", () => {
    const definitions = roleDefinitions([{ _id: "researcher", permissions: [] }]);
    assert.deepEqual(rolesFromDocument({ roles: ["sysadmin", "sysadmin", "researcher"] }, definitions), ["sysadmin", "researcher"]);
    assert.deepEqual(rolesFromDocument({ roles: ["deleted-role", "toString", "__proto__", 1, null] }, definitions), []);
});

test("no document, or one without a roles array, grants nothing", () => {
    assert.deepEqual(rolesFromDocument(null), []);
    assert.deepEqual(rolesFromDocument({ roles: "sysadmin" }), []);
});

test("permissions are the union of the roles'", () => {
    const definitions = roleDefinitions([
        { _id: "sysadmin", permissions: [] },
        { _id: "researcher", permissions: ["sead_agent"] },
    ]);
    assert.deepEqual(permissionsOfRoles(["sysadmin"], definitions), ["administer_users"]);
    assert.deepEqual(permissionsOfRoles(["researcher", "sysadmin"], definitions), ["administer_users", "sead_agent"]);
    assert.deepEqual(permissionsOfRoles(["gone"], definitions), []);
});

test("a signed-out request has no roles, without asking Mongo", async () => {
    const roles = new UserRoles({ get mongoReady() { throw new Error("Mongo was asked"); } });
    assert.deepEqual(await roles.rolesOf(null), []);
    assert.deepEqual(await roles.accessOf(null), { roles: [], permissions: [] });
});

test("roles are read from user_roles by user id, and permissions from roles", async () => {
    const asked = [];
    const app = {
        mongoReady: Promise.resolve(),
        mongo: {
            collection: name => ({
                findOne: async query => {
                    asked.push([name, query]);
                    return { _id: query._id, roles: ["sysadmin", "researcher"] };
                },
                find: () => ({ toArray: async () => [{ _id: "researcher", permissions: ["sead_agent"] }, { _id: "sysadmin", permissions: [] }] }),
            }),
        },
    };
    assert.deepEqual(await new UserRoles(app).accessOf("saml:abc@umu.se"), {
        roles: ["sysadmin", "researcher"],
        permissions: ["administer_users", "sead_agent"],
    });
    assert.deepEqual(asked, [["user_roles", { _id: "saml:abc@umu.se" }]]);
});

test("user, the default role, is built in and allows nothing until an admin decides", () => {
    const user = roleDefinitions().get("user");
    assert.equal(user.builtin, true);
    assert.deepEqual(user.permissions, []);
    assert.deepEqual(user.locked, []);
    assert.deepEqual(roleDefinitions([{ _id: "user", permissions: ["sead_agent"] }]).get("user").permissions, ["sead_agent"]);
});
