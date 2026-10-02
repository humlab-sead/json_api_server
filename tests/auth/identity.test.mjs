/**
 * The login providers' user objects (plans/sead-login-plan.md §4): the SAML
 * hand-off's header decoding, the ORCID user, user ids and attribution.
 * Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import {
    decodeHeader, splitMultiValued, samlUserFromHeaders, orcidUserFromClaims,
    userIdOf, attributionOf, safeReturnPath,
} from "../../src/Lib/Auth/AuthIdentity.js";

/** A header value as Node hands it over: the UTF-8 bytes read as latin-1. */
const asReceived = s => Buffer.from(s, "utf8").toString("latin1");

test("header values are re-decoded from UTF-8", () => {
    assert.equal(decodeHeader(asReceived("Åsa Öberg")), "Åsa Öberg");
    assert.equal(decodeHeader(asReceived("plain")), "plain");
    assert.equal(decodeHeader(undefined), "");
});

test("multi-valued attributes are split on ; and \\; is kept as a ;", () => {
    assert.deepEqual(splitMultiValued("member@umu.se;staff@umu.se"), ["member@umu.se", "staff@umu.se"]);
    assert.deepEqual(splitMultiValued("a\\;b;c"), ["a;b", "c"]);
    assert.deepEqual(splitMultiValued(""), []);
    assert.deepEqual(splitMultiValued("one"), ["one"]);
});

test("a SAML user is keyed on subject-id, with the attributes normalised", () => {
    const user = samlUserFromHeaders({
        "subject-id": "alice@sead-idp.local",
        "eppn": "alice@sead-idp.local",
        "displayname": asReceived("Åsa Öberg"),
        "mail": "asa@example.org",
        "affiliation": "member@sead-idp.local;staff@sead-idp.local",
        "schachomeorganization": "sead-idp.local",
        "shib-identity-provider": "https://sead-idp.local/simplesaml/module.php/saml/idp/metadata",
    });
    assert.deepEqual(user, {
        provider: "saml",
        issuer: "https://sead-idp.local/simplesaml/module.php/saml/idp/metadata",
        id: "alice@sead-idp.local",
        displayName: "Åsa Öberg",
        emails: [{ value: "asa@example.org" }],
        affiliation: ["member@sead-idp.local", "staff@sead-idp.local"],
        organization: "sead-idp.local",
    });
    assert.equal(userIdOf(user), "saml:alice@sead-idp.local");
});

test("a SAML user without subject-id falls back to eppn, and without a name to givenName + sn", () => {
    const user = samlUserFromHeaders({ "eppn": "bob@sead-idp.local", "givenname": "Bob", "sn": "Builder" });
    assert.equal(user.id, "bob@sead-idp.local");
    assert.equal(user.displayName, "Bob Builder");
    assert.deepEqual(user.emails, []);
    assert.equal(userIdOf(user), "saml:bob@sead-idp.local");
});

test("a SAML login without any identifier is refused", () => {
    assert.equal(samlUserFromHeaders({ "displayname": "Nobody", "mail": "x@example.org" }), null);
});

test("an ORCID user has no email, and falls back to the iD for a name", () => {
    const named = orcidUserFromClaims({ iss: "https://sandbox.orcid.org", sub: "0000-0002-1825-0097", given_name: "Josiah", family_name: "Carberry" });
    assert.equal(named.displayName, "Josiah Carberry");
    assert.equal(named.uri, "https://sandbox.orcid.org/0000-0002-1825-0097");
    assert.deepEqual(named.emails, []);
    assert.equal(userIdOf(named), "orcid:0000-0002-1825-0097");

    const unnamed = orcidUserFromClaims({ iss: "https://orcid.org", sub: "0000-0002-1825-0097" });
    assert.equal(unnamed.displayName, "0000-0002-1825-0097");
});

test("Google and GitHub keep the email-provider id, and a user with no email gets none", () => {
    assert.equal(userIdOf({ provider: "google", emails: [{ value: "a@example.org", verified: true }] }), "a@example.org-google");
    assert.equal(userIdOf({ provider: "github", emails: ["b@example.org"] }), "b@example.org-github");
    assert.equal(userIdOf({ provider: "github", emails: [] }), null);
    assert.equal(userIdOf(null), null);
});

test("attribution prefers the ORCID iD URI, otherwise name and email", () => {
    assert.equal(attributionOf(orcidUserFromClaims({ iss: "https://orcid.org", sub: "0000-0002-1825-0097", given_name: "Josiah", family_name: "Carberry" })),
        "Josiah Carberry <https://orcid.org/0000-0002-1825-0097>");
    assert.equal(attributionOf(orcidUserFromClaims({ iss: "https://orcid.org", sub: "0000-0002-1825-0097" })),
        "<https://orcid.org/0000-0002-1825-0097>");
    assert.equal(attributionOf({ provider: "saml", id: "x", displayName: "Åsa Öberg", emails: [{ value: "asa@example.org" }] }),
        "Åsa Öberg <asa@example.org>");
    assert.equal(attributionOf({ provider: "saml", id: "x", displayName: "Åsa Öberg", emails: [] }), "Åsa Öberg");
    assert.equal(attributionOf(null), null);
});

test("the full-page login only returns to a path on this site", () => {
    assert.equal(safeReturnPath("/viewstate/abc?x=1"), "/viewstate/abc?x=1");
    assert.equal(safeReturnPath("//evil.example/"), null);
    assert.equal(safeReturnPath("/\\evil.example/"), null);
    assert.equal(safeReturnPath("https://evil.example/"), null);
    assert.equal(safeReturnPath("/ok\r\nSet-Cookie: x"), null);
    assert.equal(safeReturnPath(undefined), null);
});
