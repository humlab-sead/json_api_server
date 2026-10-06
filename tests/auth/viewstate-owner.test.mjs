/**
 * Who a viewstate belongs to: the token Viewstates stores for a user has to be the one
 * the old viewstate server (sqs_viewstate_server) stored, or Google users lose what
 * they saved there. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "crypto";
import Viewstates from "../../src/EndpointModules/Viewstates.class.js";
import { userIdOf } from "../../src/Lib/Auth/AuthIdentity.js";

test("a Google user's viewstate owner is the old server's sha1(email + salt)", () => {
    const salt = "the-old-servers-salt";
    const previous = process.env.JAS_AUTH_SALT;
    process.env.JAS_AUTH_SALT = salt;
    try {
        const user = { provider: "google", emails: [{ value: "ada@example.org" }] };
        //sqs_viewstate_server, src/user.js: sha1(payload.email + config.security.salt)
        const oldToken = crypto.createHash("sha1").update("ada@example.org" + salt).digest("hex");
        assert.equal(Viewstates.prototype.getUserToken(userIdOf(user)), oldToken);
    }
    finally {
        if (previous === undefined) delete process.env.JAS_AUTH_SALT;
        else process.env.JAS_AUTH_SALT = previous;
    }
});
