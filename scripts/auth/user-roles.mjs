#!/usr/bin/env node
/**
 * Grants and revokes the roles of signed-in users (src/Lib/Auth/UserRoles.js), as the
 * client's admin panel does. This is for when there is no admin to use the panel yet.
 * What each role may do is decided in the panel; "list" shows it.
 *
 *   node scripts/auth/user-roles.mjs list
 *   node scripts/auth/user-roles.mjs signed-in
 *   node scripts/auth/user-roles.mjs grant  <user-id> <role> [note]
 *   node scripts/auth/user-roles.mjs revoke <user-id> <role>
 *
 * A user id is the key the login gives the user: saml:<subject-id or eppn>, or
 * orcid:<iD>. "signed-in" lists everyone with a live session and their id, so a
 * new sysadmin can sign in once and then be found there. The server also logs
 * "User <id> signed in with <provider>" at each login.
 *
 * Reads the same MONGO_HOST / _USER / _PASS / _DB as the server, so run it in the
 * container:
 *
 *   podman compose exec json_api_server node scripts/auth/user-roles.mjs list
 */
import { MongoClient } from "mongodb";
import { USER_ROLES_COLLECTION, ROLES_COLLECTION, roleDefinitions, rolesFromDocument } from "../../src/Lib/Auth/UserRoles.js";
import { isUserId } from "../../src/Lib/Auth/AuthIdentity.js";
import { liveSessionsByUser } from "../../src/Lib/Auth/Sessions.js";

const USAGE = `Usage:
  user-roles.mjs list
  user-roles.mjs signed-in
  user-roles.mjs grant  <user-id> <role> [note]
  user-roles.mjs revoke <user-id> <role>

"list" shows the roles there are.`;

function fail(message) {
    console.error(message);
    console.error("");
    console.error(USAGE);
    process.exit(2);
}

async function definitionsIn(db) {
    return roleDefinitions(await db.collection(ROLES_COLLECTION).find({}).toArray());
}

async function checkArgs(db, userId, role) {
    if (!userId || !role) fail("Both a user id and a role are needed.");
    if (!isUserId(userId)) fail(`"${userId}" is not a user id (saml:<id> or orcid:<iD>).`);
    if (!(await definitionsIn(db)).has(role)) fail(`There is no role "${role}".`);
}

async function list(db) {
    const definitions = await definitionsIn(db);
    console.log("Roles:");
    for (const role of definitions.values()) {
        console.log(`  ${role.id.padEnd(16)} ${role.permissions.join(", ") || "-"}${role.description ? `  (${role.description})` : ""}`);
    }
    console.log("");
    const docs = await db.collection(USER_ROLES_COLLECTION).find({}).sort({ _id: 1 }).toArray();
    if (docs.length === 0) {
        console.log("No user has a role.");
        return;
    }
    for (const doc of docs) {
        const roles = rolesFromDocument(doc, definitions);
        const note = doc.note ? `  (${doc.note})` : "";
        console.log(`${doc._id}  ${roles.length ? roles.join(", ") : "-"}${note}`);
    }
}

async function signedIn(db) {
    const users = new Map([...await liveSessionsByUser(db)].map(([id, { user }]) => [id, user]));
    if (users.size === 0) {
        console.log("Nobody is signed in.");
        return;
    }
    const roleDocs = await db.collection(USER_ROLES_COLLECTION).find({ _id: { $in: [...users.keys()] } }).toArray();
    const definitions = await definitionsIn(db);
    const rolesById = new Map(roleDocs.map(doc => [doc._id, rolesFromDocument(doc, definitions)]));
    for (const [id, user] of [...users].sort(([a], [b]) => a.localeCompare(b))) {
        const roles = rolesById.get(id) || [];
        const org = user.organization ? `, ${user.organization}` : "";
        console.log(`${id}  ${user.displayName || ""}${org}${roles.length ? `  [${roles.join(", ")}]` : ""}`);
    }
}

async function grant(db, userId, role, note) {
    await checkArgs(db, userId, role);
    const update = { $addToSet: { roles: role }, $set: { updated_at: new Date() } };
    if (note) update.$set.note = note;
    await db.collection(USER_ROLES_COLLECTION).updateOne({ _id: userId }, update, { upsert: true });
    console.log(`${userId} now has the role ${role}.`);
}

async function revoke(db, userId, role) {
    await checkArgs(db, userId, role);
    const collection = db.collection(USER_ROLES_COLLECTION);
    const result = await collection.updateOne({ _id: userId }, { $pull: { roles: role }, $set: { updated_at: new Date() } });
    if (result.matchedCount === 0) {
        console.log(`${userId} has no roles.`);
        return;
    }
    //a user without roles or note needs no document
    await collection.deleteOne({ _id: userId, roles: { $size: 0 }, note: { $exists: false } });
    console.log(`${userId} no longer has the role ${role}.`);
}

const [command, ...args] = process.argv.slice(2);
const commands = { list, "signed-in": signedIn, grant, revoke };
if (!Object.hasOwn(commands, command)) fail(command ? `Unknown command "${command}".` : "No command given.");

for (const name of ["MONGO_HOST", "MONGO_USER", "MONGO_PASS", "MONGO_DB"]) {
    if (!process.env[name]) fail(`${name} is not set. Run this in the json_api_server container.`);
}
const client = new MongoClient(`mongodb://${encodeURIComponent(process.env.MONGO_USER)}:${encodeURIComponent(process.env.MONGO_PASS)}@${process.env.MONGO_HOST}:27017/`);
try {
    await client.connect();
    await commands[command](client.db(process.env.MONGO_DB), ...args);
}
finally {
    await client.close();
}
