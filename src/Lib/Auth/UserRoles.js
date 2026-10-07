/**
 * What a signed-in user may do beyond what anyone can.
 *
 * A permission is one thing a user may do. There is a fixed set of them, in PERMISSIONS,
 * because each is checked by code somewhere.
 *
 * A role is a named set of permissions, which an admin decides on in the client's admin
 * panel. Roles are kept in this server's Mongo, collection roles:
 *
 *   { _id: "data-manager", description: "...", permissions: ["sead_agent"],
 *     updated_at: Date, updated_by: "saml:admin@umu.se" }
 *
 * Two roles are built in (BUILTIN_ROLES): they exist without a document and cannot be
 * deleted. sysadmin always has the permissions in its locked list - so there is always a
 * role that can administer users - and is the role SDF import asks for
 * (SeadDataFormat.class.js). user (DEFAULT_ROLE) is given to everyone the first time they
 * sign in and accept the privacy policy (AuthenticationHandler.class.js); what it allows is
 * up to the admins, and it allows nothing until they decide.
 *
 * Users are given roles in collection user_roles, one document per user:
 *
 *   { _id: "orcid:0000-0002-1825-0097", roles: ["sysadmin"], note: "...",
 *     updated_at: Date, updated_by: "saml:admin@umu.se" }
 *
 * _id is the key userIdOf gives the user (AuthIdentity.js), the same key their
 * viewstates are stored under. Roles and permissions are looked up on every request that
 * needs them rather than kept in the session, so a revoked one stops working at once.
 *
 * Grant and revoke them in the admin panel (EndpointModules/UserAdmin.class.js), or with
 * scripts/auth/user-roles.mjs.
 */

export const USER_ROLES_COLLECTION = "user_roles";
export const ROLES_COLLECTION = "roles";

/** The permissions there are, and what each one allows. */
export const PERMISSIONS = {
    administer_users: {
        label: "Administer users",
        description: "Use the admin panel: give users roles and take them away, and decide what each role may do.",
    },
    sead_agent: {
        label: "SEAD agent",
        description: "Use the SEAD agent chatbox.",
    },
};

/** The role SDF import asks for. */
export const ADMIN_ROLE = "sysadmin";
/** The role everyone is given at their first sign-in. */
export const DEFAULT_ROLE = "user";

/** Roles that exist without a document. Their locked permissions cannot be taken away. */
export const BUILTIN_ROLES = {
    sysadmin: {
        description: "Administers SEAD, and imports data (SDF workbooks).",
        permissions: ["administer_users", "sead_agent"],
        locked: ["administer_users"],
    },
    user: {
        description: "Everyone who has signed in. Given at the first sign-in.",
        permissions: [],
        locked: [],
    },
};

/** A role id: lowercase, starting with a letter, as it is shown in the panel. */
export const ROLE_ID_PATTERN = /^[a-z][a-z0-9_-]{0,31}$/;
export const MAX_ROLE_DESCRIPTION_LENGTH = 300;

function knownPermissions(permissions) {
    if (!Array.isArray(permissions)) return [];
    return [...new Set(permissions.filter(p => typeof p === "string" && Object.hasOwn(PERMISSIONS, p)))];
}

/**
 * Every role there is: the built-in ones, with what their documents say, and the stored
 * ones. id -> { id, description, permissions, builtin, locked }.
 */
export function roleDefinitions(docs = []) {
    const definitions = new Map();
    for (const [id, builtin] of Object.entries(BUILTIN_ROLES)) {
        definitions.set(id, { id, description: builtin.description, permissions: [...builtin.permissions], builtin: true, locked: [...builtin.locked] });
    }
    for (const doc of docs) {
        if (!doc || typeof doc._id !== "string" || !ROLE_ID_PATTERN.test(doc._id)) continue;
        const builtin = BUILTIN_ROLES[doc._id];
        const locked = builtin ? builtin.locked : [];
        definitions.set(doc._id, {
            id: doc._id,
            description: typeof doc.description === "string" ? doc.description : (builtin ? builtin.description : ""),
            permissions: knownPermissions([...locked, ...knownPermissions(doc.permissions)]),
            builtin: !!builtin,
            locked: [...locked],
            updated_at: doc.updated_at || null,
            updated_by: doc.updated_by || null,
        });
    }
    return definitions;
}

/** The roles a user_roles document grants: the ones that exist, once each. */
export function rolesFromDocument(doc, definitions = roleDefinitions()) {
    if (!doc || !Array.isArray(doc.roles)) return [];
    return [...new Set(doc.roles.filter(role => typeof role === "string" && definitions.has(role)))];
}

/** What these roles allow together. */
export function permissionsOfRoles(roles, definitions = roleDefinitions()) {
    const permissions = new Set();
    for (const role of roles) {
        const definition = definitions.get(role);
        if (definition) definition.permissions.forEach(p => permissions.add(p));
    }
    return Object.keys(PERMISSIONS).filter(p => permissions.has(p));
}

export default class UserRoles {
    constructor(app) {
        this.app = app;
    }

    get collection() {
        return this.app.mongo.collection(USER_ROLES_COLLECTION);
    }

    get rolesCollection() {
        return this.app.mongo.collection(ROLES_COLLECTION);
    }

    /** Every role there is (roleDefinitions). Throws if Mongo cannot be read. */
    async definitions() {
        await this.app.mongoReady;
        return roleDefinitions(await this.rolesCollection.find({}).toArray());
    }

    /** The roles of the user with this id; none for no id. Throws if Mongo cannot be read. */
    async rolesOf(userId) {
        return (await this.accessOf(userId)).roles;
    }

    /** The roles of the user with this id, and the permissions they give. */
    async accessOf(userId) {
        if (!userId) return { roles: [], permissions: [] };
        await this.app.mongoReady;
        const [doc, definitions] = await Promise.all([this.collection.findOne({ _id: userId }), this.definitions()]);
        const roles = rolesFromDocument(doc, definitions);
        return { roles, permissions: permissionsOfRoles(roles, definitions) };
    }

    /** Every user_roles document. */
    async all() {
        await this.app.mongoReady;
        return this.collection.find({}).toArray();
    }

    /**
     * Gives the user exactly these roles, and this note when one is given (an empty
     * note removes it). A user left with neither roles nor note needs no document.
     * The roles must exist; the caller checks.
     */
    async setRoles(userId, roles, { note, by } = {}) {
        await this.app.mongoReady;
        const set = { roles: [...new Set(roles)], updated_at: new Date(), updated_by: by || null };
        const update = { $set: set };
        if (typeof note === "string") {
            if (note.trim()) set.note = note.trim();
            else update.$unset = { note: "" };
        }
        await this.collection.updateOne({ _id: userId }, update, { upsert: true });
        await this.collection.deleteOne({ _id: userId, roles: { $size: 0 }, note: { $exists: false } });
        return rolesFromDocument(await this.collection.findOne({ _id: userId }), await this.definitions());
    }

    /** Gives the user one more role, keeping the ones they have. */
    async addRole(userId, role, by = null) {
        await this.app.mongoReady;
        await this.collection.updateOne(
            { _id: userId },
            { $addToSet: { roles: role }, $set: { updated_at: new Date(), updated_by: by } },
            { upsert: true }
        );
    }

    /** Forgets the user's roles, and the note on them. */
    async deleteUser(userId) {
        await this.app.mongoReady;
        await this.collection.deleteOne({ _id: userId });
    }

    /**
     * Stores a role's description and permissions, creating the role if it does not exist.
     * The permissions must be known ones; a built-in role keeps its locked ones whatever is given.
     */
    async saveRole(roleId, { description, permissions }, by) {
        await this.app.mongoReady;
        await this.rolesCollection.updateOne(
            { _id: roleId },
            { $set: { description, permissions: knownPermissions(permissions), updated_at: new Date(), updated_by: by || null } },
            { upsert: true }
        );
        return (await this.definitions()).get(roleId);
    }

    /** Deletes a role, and takes it away from everyone who had it. Not for built-in roles. */
    async deleteRole(roleId) {
        await this.app.mongoReady;
        await this.rolesCollection.deleteOne({ _id: roleId });
        const result = await this.collection.updateMany({ roles: roleId }, { $pull: { roles: roleId }, $set: { updated_at: new Date() } });
        await this.collection.deleteMany({ roles: { $size: 0 }, note: { $exists: false } });
        return result.modifiedCount;
    }
}
