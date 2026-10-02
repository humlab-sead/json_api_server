/**
 * The user objects the login providers produce, and what the rest of the server
 * reads from them: a stable id, a display name and an attribution string.
 *
 * Every provider is normalised to the same shape:
 *   { provider, issuer, id, displayName, emails: [{ value }], ... }
 * SAML users also carry affiliation and organization, ORCID users their iD as a URI.
 */

/**
 * The attribute headers the router copies onto the SAML hand-off, by the ids
 * router/shibboleth/attribute-map.xml gives them. The router clears every one of
 * these from the client's request before shib_request runs, so their presence
 * here means shibd put them there.
 */
export const SAML_HEADERS = {
    subjectId: "subject-id",
    eppn: "eppn",
    displayName: "displayname",
    givenName: "givenname",
    surname: "sn",
    mail: "mail",
    affiliation: "affiliation",
    organization: "schachomeorganization",
    issuer: "shib-identity-provider",
};

/**
 * Node reads header values as latin-1, one character per byte. shibd sends UTF-8,
 * so "Åsa" arrives as "Ã\u0085sa" and has to be put back together from its bytes.
 */
export function decodeHeader(value) {
    if (typeof value !== "string" || value === "") return "";
    return Buffer.from(value, "latin1").toString("utf8");
}

/**
 * shibd joins the values of a multi-valued attribute with ";", and escapes a ";"
 * inside a value as "\;".
 */
export function splitMultiValued(value) {
    if (!value) return [];
    const values = [];
    let current = "";
    for (let i = 0; i < value.length; i++) {
        const ch = value[i];
        if (ch === "\\" && value[i + 1] === ";") {
            current += ";";
            i++;
        }
        else if (ch === ";") {
            values.push(current);
            current = "";
        }
        else {
            current += ch;
        }
    }
    values.push(current);
    return values.map(v => v.trim()).filter(v => v !== "");
}

/**
 * The user the SAML hand-off logs in, from the attribute headers the router passed
 * on. Returns null when there is no stable subject to key the user on.
 */
export function samlUserFromHeaders(headers) {
    const values = name => splitMultiValued(decodeHeader(headers[name]));
    const first = name => values(name)[0] || null;

    const id = first(SAML_HEADERS.subjectId) || first(SAML_HEADERS.eppn);
    if (!id) return null;

    const givenName = first(SAML_HEADERS.givenName);
    const surname = first(SAML_HEADERS.surname);
    const displayName = first(SAML_HEADERS.displayName)
        || [givenName, surname].filter(Boolean).join(" ")
        || id;
    const mail = first(SAML_HEADERS.mail);

    return {
        provider: "saml",
        issuer: first(SAML_HEADERS.issuer),
        id,
        displayName,
        emails: mail ? [{ value: mail }] : [],
        affiliation: values(SAML_HEADERS.affiliation),
        organization: first(SAML_HEADERS.organization),
    };
}

/**
 * The user an ORCID login produces, from the claims of its validated id_token.
 * ORCID never releases an email, and a user may keep their name private.
 */
export function orcidUserFromClaims(claims) {
    const name = [claims.given_name, claims.family_name].filter(Boolean).join(" ");
    const issuer = String(claims.iss).replace(/\/+$/, "");
    return {
        provider: "orcid",
        issuer,
        id: claims.sub,
        //ORCID's display guidelines: an authenticated iD is shown as its full URI
        uri: `${issuer}/${claims.sub}`,
        displayName: name || claims.sub,
        emails: [],
    };
}

/** The first email of a user, whichever form the provider gave it in. */
export function primaryEmail(user) {
    if (!user || !Array.isArray(user.emails) || user.emails.length === 0) return null;
    const email = user.emails[0];
    return (typeof email === "string" ? email : email && email.value) || null;
}

/**
 * The key a user's data (viewstates) is stored under. SAML and ORCID users are keyed
 * on their stable subject. Google and GitHub keep the email-provider form they have
 * always had, so what was saved under them is not orphaned.
 */
export function userIdOf(user) {
    if (!user || !user.provider) return null;
    if (user.provider === "saml" || user.provider === "orcid") {
        return user.id ? `${user.provider}:${user.id}` : null;
    }
    const email = primaryEmail(user);
    return email ? `${email}-${user.provider}` : null;
}

/** Who exported or submitted something: "Name <orcid uri>", or "Name <email>". */
export function attributionOf(user) {
    if (!user) return null;
    if (user.uri) {
        const name = user.displayName && user.displayName !== user.id ? user.displayName : null;
        return [name, `<${user.uri}>`].filter(Boolean).join(" ");
    }
    const email = primaryEmail(user);
    return [user.displayName, email && `<${email}>`].filter(Boolean).join(" ") || null;
}

/**
 * A return path for the full-page login fallback. Only a path on this site is
 * accepted, so the login cannot be used as an open redirect.
 */
export function safeReturnPath(value) {
    if (typeof value !== "string" || value === "") return null;
    if (!value.startsWith("/") || value.startsWith("//") || value.startsWith("/\\")) return null;
    if (/[\u0000-\u001f]/.test(value)) return null;
    return value;
}
