import { userIdOf, primaryEmail } from "./AuthIdentity.js";

/**
 * The accounts of SEAD: everyone who has signed in and accepted the privacy policy. Kept in
 * this server's Mongo, collection users, one document per user, with what the login
 * provider gave:
 *
 *   { _id: "saml:alice@umu.se", provider: "saml", display_name: "Alice", email: "...",
 *     organization: "umu.se", uri: null, first_sign_in_at: Date, last_sign_in_at: Date,
 *     sign_ins: 12, privacy_consent: { version: "2026-10-07", at: Date } }
 *
 * _id is the key userIdOf gives the user, the same key as in user_roles.
 *
 * Nothing is written here until the user has accepted the privacy policy (recordConsent).
 * Until then a login is only a session, which expires on its own, and the user has no roles.
 * Each sign-in after that updates the document (recordSignIn). The policy is the client's
 * (src/index.ejs, #gdpr-infobox); PRIVACY_POLICY_VERSION is the version of its text that
 * users have to have accepted, and has to be raised along with it whenever it changes, which
 * asks everyone to accept it again.
 */

export const USERS_COLLECTION = "users";
export const PRIVACY_POLICY_VERSION = "2026-10-07";

/** What is kept of a user: who they are, not the whole login. */
export function profileOf(user) {
    return {
        provider: user.provider,
        display_name: user.displayName || null,
        email: primaryEmail(user),
        organization: user.organization || null,
        uri: user.uri || null,
    };
}

/** Whether a users document holds consent to the current privacy policy. */
export function hasCurrentConsent(doc) {
    return !!(doc && doc.privacy_consent && doc.privacy_consent.version === PRIVACY_POLICY_VERSION);
}

export default class UserDirectory {
    constructor(app) {
        this.app = app;
    }

    get collection() {
        return this.app.mongo.collection(USERS_COLLECTION);
    }

    /** The user's document, or null. Throws if Mongo cannot be read. */
    async get(userId) {
        if (!userId) return null;
        await this.app.mongoReady;
        return this.collection.findOne({ _id: userId });
    }

    /** Whether the user has accepted the current privacy policy. */
    async hasConsented(userId) {
        return hasCurrentConsent(await this.get(userId));
    }

    /**
     * Records a sign-in, for a user who has an account. Someone who has never accepted
     * the policy has none, and nothing is written. Throws if Mongo cannot be written.
     */
    async recordSignIn(user, now = new Date()) {
        const userId = userIdOf(user);
        if (!userId) return;
        await this.app.mongoReady;
        await this.collection.updateOne(
            { _id: userId, privacy_consent: { $exists: true } },
            { $set: { ...profileOf(user), last_sign_in_at: now }, $inc: { sign_ins: 1 } }
        );
    }

    /**
     * Records that the user accepted this version of the privacy policy, which creates their
     * account if they had none. Returns whether it was their first acceptance - their first
     * sign-in, as far as their account goes.
     */
    async recordConsent(user, version, now = new Date()) {
        const userId = userIdOf(user);
        await this.app.mongoReady;
        const before = await this.collection.findOneAndUpdate(
            { _id: userId },
            {
                $set: { ...profileOf(user), privacy_consent: { version, at: now } },
                $setOnInsert: { first_sign_in_at: now, last_sign_in_at: now, sign_ins: 1 },
            },
            { upsert: true, returnDocument: "before" }
        );
        return !(before && before.privacy_consent);
    }

    /** Forgets the user's account. */
    async deleteUser(userId) {
        await this.app.mongoReady;
        await this.collection.deleteOne({ _id: userId });
    }

    async all() {
        await this.app.mongoReady;
        return this.collection.find({}).toArray();
    }
}
