import { PERMISSIONS, ROLE_ID_PATTERN, MAX_ROLE_DESCRIPTION_LENGTH, rolesFromDocument, permissionsOfRoles } from "../Lib/Auth/UserRoles.js";
import { isUserId } from "../Lib/Auth/AuthIdentity.js";
import { profileOf } from "../Lib/Auth/UserDirectory.js";
import { liveSessionsByUser, endSessionsOf } from "../Lib/Auth/Sessions.js";

/**
 * The client's admin panel: the users of SEAD, their roles, and what each role may do.
 *
 *   GET    /admin/users                     every known user, their roles and whether they
 *                                           are signed in
 *   PUT    /admin/users/:userId             { roles: [..], note? } - gives the user exactly
 *                                           these roles
 *   DELETE /admin/users/:userId/sessions    signs the user out everywhere
 *
 *   GET    /admin/roles                     the roles, the permissions there are, and how
 *                                           many users have each role
 *   POST   /admin/roles                     { id, description, permissions } - a new role
 *   PUT    /admin/roles/:roleId             { description, permissions } - changes a role
 *   DELETE /admin/roles/:roleId             deletes a role, and takes it from its users
 *
 * A known user is one who has an account (users: signed in and accepted the privacy policy)
 * or a role (user_roles). A role can be given to a user id that has neither, so an account
 * can be set up before its first sign-in.
 *
 * Only for a signed-in user with the administer_users permission. The protected-endpoint
 * password does not open these: it would let anyone who has it hand out roles under no
 * name. Scripts have scripts/auth/user-roles.mjs. No change is made that would take
 * administer_users from the admin making it, so there is always someone left who has it.
 * Every change is logged with who made it, and the documents keep who made the last one.
 */

const ADMIN_PERMISSION = "administer_users";
const MAX_NOTE_LENGTH = 500;

/** The provider a user id was made for: saml:.., orcid:.., ..-google, ..-github. */
function providerOfUserId(id) {
    const match = id.match(/^(saml|orcid):|-(google|github)$/);
    return match ? match[1] || match[2] : null;
}

/** What the panel lists for one user, from what each collection knows of them. */
export function userEntry(id, { directory, roleDoc, session }, definitions) {
    const profile = directory || (session ? profileOf(session.user) : {});
    return {
        id,
        provider: profile.provider || providerOfUserId(id),
        display_name: profile.display_name || null,
        email: profile.email || null,
        organization: profile.organization || null,
        uri: profile.uri || null,
        first_sign_in_at: directory ? directory.first_sign_in_at || null : null,
        last_sign_in_at: directory ? directory.last_sign_in_at || null : null,
        sign_ins: directory ? directory.sign_ins || 0 : 0,
        signed_in: session != null,
        roles: rolesFromDocument(roleDoc, definitions),
        note: roleDoc && roleDoc.note ? roleDoc.note : null,
        roles_updated_at: roleDoc ? roleDoc.updated_at || null : null,
        roles_updated_by: roleDoc ? roleDoc.updated_by || null : null,
    };
}

/** What the panel lists for one role. */
export function roleEntry(definition, users) {
    return {
        id: definition.id,
        description: definition.description,
        permissions: definition.permissions,
        builtin: definition.builtin,
        locked: definition.locked,
        users,
        updated_at: definition.updated_at || null,
        updated_by: definition.updated_by || null,
    };
}

/** The roles in a user PUT body, or an error to answer with. */
export function parseRolesBody(body, definitions) {
    if (!body || !Array.isArray(body.roles)) {
        return { error: "'roles' must be an array of role names." };
    }
    const unknown = body.roles.filter(role => typeof role !== "string" || !definitions.has(role));
    if (unknown.length) {
        return { error: `There is no role ${unknown.map(role => JSON.stringify(role)).join(", ")}.` };
    }
    if (body.note !== undefined && body.note !== null && typeof body.note !== "string") {
        return { error: "'note' must be a string." };
    }
    if (typeof body.note === "string" && body.note.length > MAX_NOTE_LENGTH) {
        return { error: `The note is longer than ${MAX_NOTE_LENGTH} characters.` };
    }
    return { roles: [...new Set(body.roles)], note: body.note === null ? "" : body.note };
}

/** The description and permissions in a role POST/PUT body, or an error to answer with. */
export function parseRoleBody(body) {
    if (!body || !Array.isArray(body.permissions)) {
        return { error: "'permissions' must be an array of permission names." };
    }
    const unknown = body.permissions.filter(p => typeof p !== "string" || !Object.hasOwn(PERMISSIONS, p));
    if (unknown.length) {
        return { error: `There is no permission ${unknown.map(p => JSON.stringify(p)).join(", ")}.` };
    }
    const description = body.description == null ? "" : body.description;
    if (typeof description !== "string") {
        return { error: "'description' must be a string." };
    }
    if (description.length > MAX_ROLE_DESCRIPTION_LENGTH) {
        return { error: `The description is longer than ${MAX_ROLE_DESCRIPTION_LENGTH} characters.` };
    }
    return { description: description.trim(), permissions: [...new Set(body.permissions)] };
}

/**
 * Whether a user with these roles still administers users once the roles are defined as
 * given. Asked of the admin making a change, about the state the change would leave.
 */
export function keepsAdmin(roles, definitions) {
    return permissionsOfRoles(roles, definitions).includes(ADMIN_PERMISSION);
}

const LOCKOUT_ERROR = {
    error: "That would take away your own permission to administer users. Another admin has to do it.",
    code: "own_admin_permission",
};

class UserAdmin {
    constructor(app) {
        this.app = app;
        this.auth = app.authHandler;

        const admin = this.auth.requirePermission(ADMIN_PERMISSION);
        const expressApp = this.app.expressApp;
        expressApp.get("/admin/users", admin, this.handleListUsers.bind(this));
        expressApp.put("/admin/users/:userId", admin, this.handleSetRoles.bind(this));
        expressApp.delete("/admin/users/:userId/sessions", admin, this.handleEndSessions.bind(this));
        expressApp.get("/admin/roles", admin, this.handleListRoles.bind(this));
        expressApp.post("/admin/roles", admin, this.handleCreateRole.bind(this));
        expressApp.put("/admin/roles/:roleId", admin, this.handleUpdateRole.bind(this));
        expressApp.delete("/admin/roles/:roleId", admin, this.handleDeleteRole.bind(this));
    }

    async handleListUsers(req, res) {
        res.set("Cache-Control", "no-store");
        try {
            await this.app.mongoReady;
            const [directory, roleDocs, sessions, definitions] = await Promise.all([
                this.auth.userDirectory.all(),
                this.auth.userRoles.all(),
                liveSessionsByUser(this.app.mongo),
                this.auth.userRoles.definitions(),
            ]);
            const byId = new Map();
            const at = id => {
                if (!byId.has(id)) byId.set(id, {});
                return byId.get(id);
            };
            directory.forEach(doc => at(doc._id).directory = doc);
            roleDocs.forEach(doc => at(doc._id).roleDoc = doc);
            //Only says who of these is signed in. Someone who is signed in but has not
            //accepted the privacy policy has no account, and is not listed.
            sessions.forEach((session, id) => {
                if (byId.has(id)) byId.get(id).session = session;
            });

            const users = [...byId].map(([id, known]) => userEntry(id, known, definitions));
            users.sort((a, b) => (a.display_name || a.id).localeCompare(b.display_name || b.id));
            res.json({ you: this.auth.getUserId(req), users });
        }
        catch (err) {
            console.error("Could not list users:", err);
            res.status(500).json({ error: "The users could not be listed." });
        }
    }

    async handleSetRoles(req, res) {
        const userId = req.params.userId;
        if (!isUserId(userId)) {
            return res.status(400).json({ error: `"${userId}" is not a user id (saml:<id>, orcid:<iD>, or <email>-google).`, code: "bad_user_id" });
        }
        const adminId = this.auth.getUserId(req);
        try {
            const definitions = await this.auth.userRoles.definitions();
            const { roles, note, error } = parseRolesBody(req.body, definitions);
            if (error) {
                return res.status(400).json({ error, code: "bad_roles" });
            }
            if (userId === adminId && !keepsAdmin(roles, definitions)) {
                return res.status(409).json(LOCKOUT_ERROR);
            }
            const before = await this.auth.userRoles.rolesOf(userId);
            const after = await this.auth.userRoles.setRoles(userId, roles, { note, by: adminId });
            const added = after.filter(role => !before.includes(role));
            const removed = before.filter(role => !after.includes(role));
            if (added.length || removed.length) {
                console.log(`${adminId} changed the roles of ${userId}:${added.map(r => " +"+r).join("")}${removed.map(r => " -"+r).join("")}`);
            }
            res.json({ id: userId, roles: after });
        }
        catch (err) {
            console.error(`Could not set the roles of ${userId}:`, err);
            res.status(500).json({ error: "The roles could not be saved." });
        }
    }

    async handleEndSessions(req, res) {
        const userId = req.params.userId;
        if (!isUserId(userId)) {
            return res.status(400).json({ error: `"${userId}" is not a user id.`, code: "bad_user_id" });
        }
        try {
            await this.app.mongoReady;
            const ended = await endSessionsOf(this.app.mongo, userId);
            console.log(`${this.auth.getUserId(req)} signed ${userId} out (${ended} session${ended == 1 ? "" : "s"})`);
            res.json({ id: userId, ended });
        }
        catch (err) {
            console.error(`Could not end the sessions of ${userId}:`, err);
            res.status(500).json({ error: "The sessions could not be ended." });
        }
    }

    async handleListRoles(req, res) {
        res.set("Cache-Control", "no-store");
        try {
            const [definitions, roleDocs] = await Promise.all([this.auth.userRoles.definitions(), this.auth.userRoles.all()]);
            const users = new Map();
            roleDocs.forEach(doc => rolesFromDocument(doc, definitions).forEach(role => users.set(role, (users.get(role) || 0) + 1)));
            const roles = [...definitions.values()].map(definition => roleEntry(definition, users.get(definition.id) || 0));
            roles.sort((a, b) => (b.builtin - a.builtin) || a.id.localeCompare(b.id));
            res.json({
                permissions: Object.entries(PERMISSIONS).map(([id, { label, description }]) => ({ id, label, description })),
                roles,
            });
        }
        catch (err) {
            console.error("Could not list roles:", err);
            res.status(500).json({ error: "The roles could not be listed." });
        }
    }

    async handleCreateRole(req, res) {
        const roleId = req.body ? req.body.id : null;
        if (typeof roleId !== "string" || !ROLE_ID_PATTERN.test(roleId)) {
            return res.status(400).json({ error: "A role name is 1 to 32 lowercase letters, digits, - and _, starting with a letter.", code: "bad_role_id" });
        }
        const { description, permissions, error } = parseRoleBody(req.body);
        if (error) {
            return res.status(400).json({ error, code: "bad_role" });
        }
        try {
            const definitions = await this.auth.userRoles.definitions();
            if (definitions.has(roleId)) {
                return res.status(409).json({ error: `There already is a role "${roleId}".`, code: "role_exists" });
            }
            const adminId = this.auth.getUserId(req);
            const saved = await this.auth.userRoles.saveRole(roleId, { description, permissions }, adminId);
            console.log(`${adminId} created the role ${roleId}: ${saved.permissions.join(", ") || "no permissions"}`);
            res.status(201).json(roleEntry(saved, 0));
        }
        catch (err) {
            console.error(`Could not create the role ${roleId}:`, err);
            res.status(500).json({ error: "The role could not be created." });
        }
    }

    async handleUpdateRole(req, res) {
        const roleId = req.params.roleId;
        const { description, permissions, error } = parseRoleBody(req.body);
        if (error) {
            return res.status(400).json({ error, code: "bad_role" });
        }
        const adminId = this.auth.getUserId(req);
        try {
            const definitions = await this.auth.userRoles.definitions();
            const current = definitions.get(roleId);
            if (!current) {
                return res.status(404).json({ error: `There is no role "${roleId}".`, code: "unknown_role" });
            }
            const missingLocked = current.locked.filter(p => !permissions.includes(p));
            if (missingLocked.length) {
                return res.status(409).json({ error: `The ${roleId} role always has ${missingLocked.map(p => PERMISSIONS[p].label).join(", ")}.`, code: "locked_permission" });
            }
            const after = new Map(definitions).set(roleId, { ...current, permissions });
            if (!keepsAdmin(await this.auth.userRoles.rolesOf(adminId), after)) {
                return res.status(409).json(LOCKOUT_ERROR);
            }
            const saved = await this.auth.userRoles.saveRole(roleId, { description, permissions }, adminId);
            const added = saved.permissions.filter(p => !current.permissions.includes(p));
            const removed = current.permissions.filter(p => !saved.permissions.includes(p));
            if (added.length || removed.length) {
                console.log(`${adminId} changed the permissions of the role ${roleId}:${added.map(p => " +"+p).join("")}${removed.map(p => " -"+p).join("")}`);
            }
            res.json(roleEntry(saved, null));
        }
        catch (err) {
            console.error(`Could not save the role ${roleId}:`, err);
            res.status(500).json({ error: "The role could not be saved." });
        }
    }

    async handleDeleteRole(req, res) {
        const roleId = req.params.roleId;
        const adminId = this.auth.getUserId(req);
        try {
            const definitions = await this.auth.userRoles.definitions();
            const current = definitions.get(roleId);
            if (!current) {
                return res.status(404).json({ error: `There is no role "${roleId}".`, code: "unknown_role" });
            }
            if (current.builtin) {
                return res.status(409).json({ error: `The ${roleId} role is built in and cannot be deleted.`, code: "builtin_role" });
            }
            const after = new Map(definitions);
            after.delete(roleId);
            if (!keepsAdmin(await this.auth.userRoles.rolesOf(adminId), after)) {
                return res.status(409).json(LOCKOUT_ERROR);
            }
            const users = await this.auth.userRoles.deleteRole(roleId);
            console.log(`${adminId} deleted the role ${roleId}, which ${users} user${users == 1 ? "" : "s"} had`);
            res.json({ id: roleId, users });
        }
        catch (err) {
            console.error(`Could not delete the role ${roleId}:`, err);
            res.status(500).json({ error: "The role could not be deleted." });
        }
    }
}

export default UserAdmin;
