/**
 * What a signed-in user may do beyond what anyone can. Roles are kept in this
 * server's Mongo, collection user_roles, one document per user:
 *
 *   { _id: "orcid:0000-0002-1825-0097", roles: ["sysadmin"], note: "...", updated_at: Date }
 *
 * _id is the key userIdOf gives the user (AuthIdentity.js), the same key their
 * viewstates are stored under. Roles are looked up on every request that needs
 * them rather than kept in the session, so a revoked role stops working at once.
 *
 * Grant and revoke them with scripts/auth/user-roles.mjs.
 */

export const USER_ROLES_COLLECTION = "user_roles";

/** The roles there are, and what each one allows. */
export const ROLES = {
    sysadmin: "Import data: validate SDF workbooks and turn them into change requests.",
};

/** The roles a user_roles document grants: its known role names, and nothing else. */
export function rolesFromDocument(doc) {
    if (!doc || !Array.isArray(doc.roles)) return [];
    return [...new Set(doc.roles.filter(role => typeof role === "string" && Object.hasOwn(ROLES, role)))];
}

export default class UserRoles {
    constructor(app) {
        this.app = app;
    }

    /** The roles of the user with this id; none for no id. Throws if Mongo cannot be read. */
    async rolesOf(userId) {
        if (!userId) return [];
        await this.app.mongoReady;
        const doc = await this.app.mongo.collection(USER_ROLES_COLLECTION).findOne({ _id: userId });
        return rolesFromDocument(doc);
    }
}
