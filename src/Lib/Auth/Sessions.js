import { userIdOf } from "./AuthIdentity.js";

/**
 * The login sessions express-session keeps in Mongo (AuthenticationHandler), read
 * from outside the session middleware: who is signed in, and ending a user's sessions.
 * connect-mongo stores each session as a JSON string under "session".
 */

export const SESSIONS_COLLECTION = "sessions";

/** The user a session document is signed in as, or null. */
export function sessionUser(doc) {
    let session;
    try {
        session = typeof doc.session === "string" ? JSON.parse(doc.session) : doc.session;
    }
    catch {
        return null;
    }
    return (session && session.passport && session.passport.user) || null;
}

/** The users with a live session: user id -> { user, sessionIds }. */
export async function liveSessionsByUser(db, now = new Date()) {
    const docs = await db.collection(SESSIONS_COLLECTION).find({ expires: { $gt: now } }).toArray();
    const users = new Map();
    for (const doc of docs) {
        const user = sessionUser(doc);
        const id = userIdOf(user);
        if (!id) continue;
        if (!users.has(id)) users.set(id, { user, sessionIds: [] });
        users.get(id).sessionIds.push(doc._id);
    }
    return users;
}

/** Signs the user out everywhere: deletes their sessions. Returns how many there were. */
export async function endSessionsOf(db, userId) {
    const live = (await liveSessionsByUser(db)).get(userId);
    if (!live) return 0;
    const result = await db.collection(SESSIONS_COLLECTION).deleteMany({ _id: { $in: live.sessionIds } });
    return result.deletedCount;
}
