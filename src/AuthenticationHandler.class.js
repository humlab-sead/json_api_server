import crypto from "crypto";
import http from "http";
import express from "express";
import session from "express-session";
import MongoStore from "connect-mongo";
import passport from "passport";
import * as oidc from "openid-client";
import { Strategy as OidcStrategy } from "openid-client/passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import { Strategy as GitHubStrategy } from "passport-github2";
import { samlUserFromHeaders, orcidUserFromClaims, googleUserFromProfile, userIdOf, safeReturnPath } from "./Lib/Auth/AuthIdentity.js";
import UserRoles from "./Lib/Auth/UserRoles.js";

//The session cookie. The __Host- prefix makes the browser refuse it unless it is
//Secure, has Path=/ and no Domain - so a login on super.sead.se can never become a
//login on browser.sead.se, or the other way round.
const SESSION_COOKIE_NAME = "__Host-sead.sid";
const SESSION_MAX_AGE_MS = 24 * 60 * 60 * 1000;
//The header the router puts the shared hand-off secret in (it clears any client copy)
const HANDOFF_SECRET_HEADER = "x-sead-sp-handoff-secret";

export default class AuthenticationHandler {
    constructor(app) {
        this.app = app;
        this.isProd = (process.env.DEPLOY_MODE || process.env.MODE) == "prod";
        //Where the browser reaches us. Callback URLs and the Origin check are built from it,
        //so the same image works on sead.local, super.sead.se and browser.sead.se.
        this.publicOrigin = (process.env.PUBLIC_ORIGIN || "https://"+(process.env.DOMAIN || "localhost")).replace(/\/+$/, "");
        this.enabledProviders = {
            saml: false,
            google: false,
            github: false,
            orcid: false
        };
        //What the sign-in dialog offers, in order. Served from /auth/status so the dialog
        //needs no client release to change its options.
        this.providers = [];
        //Which providers may be offered at all; one without credentials is left out anyway.
        //GitHub stays off until its OAuth app is registered for this domain, even where
        //credentials are present.
        this.allowedProviders = (process.env.AUTH_PROVIDERS || "saml,orcid,google").split(",").map(p => p.trim()).filter(Boolean);

        let sessionSecret = process.env.SESSION_SECRET;
        if (!sessionSecret) {
            if (this.isProd) {
                console.error('SESSION_SECRET is not set. Refusing to start in prod mode with a random session secret.');
                process.exit(1);
            }
            console.warn('⚠️  SESSION_SECRET not set. Using a random secret. Sessions will not persist across server restarts.');
            sessionSecret = this.generateRandomSecret();
        }

        //Sessions live in Mongo, so they survive restarts and don't pile up in memory
        this.sessionStore = MongoStore.create({
            clientPromise: this.app.mongoReady.then(() => this.app.mongoClient),
            dbName: process.env.MONGO_DB,
            collectionName: "sessions",
            ttl: SESSION_MAX_AGE_MS / 1000
        });

        this.sessionCookieOptions = {
            secure: true,
            httpOnly: true,
            sameSite: "lax",
            path: "/"
        };

        this.sessionMiddleware = session({
            name: SESSION_COOKIE_NAME,
            secret: sessionSecret,
            store: this.sessionStore,
            resave: false,
            saveUninitialized: false,
            cookie: { ...this.sessionCookieOptions, maxAge: SESSION_MAX_AGE_MS }
        });

        this.installSessionSupport(this.app.expressApp);
        this.userRoles = new UserRoles(this.app);

        // Serialize user - only the normalised fields, whatever else the provider returned
        passport.serializeUser((user, done) => {
            done(null, {
                provider: user.provider,
                issuer: user.issuer || null,
                id: user.id,
                uri: user.uri || null,
                displayName: user.displayName,
                emails: (user.emails || []).map(email => ({ value: typeof email === "string" ? email : email.value })),
                affiliation: user.affiliation || [],
                organization: user.organization || null,
                photos: Array.isArray(user.photos) && user.photos.length ? [{ value: user.photos[0].value }] : []
            });
        });

        // Deserialize user
        passport.deserializeUser((user, done) => {
            done(null, user);
        });

        this.setupSaml();
        this.setupOrcid();
        this.setupGoogle();
        this.setupGitHub();

        this.app.expressApp.get('/auth/status', async (req, res) => {
            res.set("Cache-Control", "no-store");
            const status = {
                loggedIn: req.isAuthenticated(),
                providers: this.providers,
                enabledProviders: this.enabledProviders
            };
            if (status.loggedIn) {
                status.user = req.user;
                //What the client offers (e.g. "Import data"). The endpoints check again themselves.
                status.roles = await this.getRolesOrNone(req);
            }
            res.json(status);
        });

        this.app.expressApp.post('/auth/logout', this.requireSameOrigin.bind(this), (req, res) => {
            req.logout((err) => {
                if (err) {
                    return res.status(500).json({ error: 'Logout failed' });
                }
                req.session.destroy(() => {
                    res.clearCookie(SESSION_COOKIE_NAME, this.sessionCookieOptions);
                    res.json({ message: 'Logged out successfully' });
                });
            });
        });

        // Log authentication status
        const enabled = Object.keys(this.enabledProviders).filter(k => this.enabledProviders[k]);
        if (enabled.length === 0) {
            console.warn('⚠️  No authentication providers configured. Server will run without authentication.');
        } else {
            console.log(`✓ Authentication enabled for: ${enabled.join(', ')}`);
        }
    }

    /**
     * Sessions and Passport on an express app. Both listeners get them, sharing the
     * one store, so a login made on the hand-off listener is a login on the API.
     */
    installSessionSupport(expressApp) {
        expressApp.set('trust proxy', this.trustProxySetting());
        expressApp.use(this.sessionMiddleware);
        expressApp.use(passport.initialize());
        expressApp.use(passport.session());
    }

    /**
     * Which proxies are believed about the client's address and scheme. There are two in
     * front of us (the router, and the host nginx or front proxy before it), both on
     * private addresses. It has to be right: the session cookie is Secure, and is only
     * set when X-Forwarded-Proto says the browser came in over https.
     */
    trustProxySetting() {
        const value = process.env.TRUST_PROXY || "loopback, uniquelocal";
        if (/^\d+$/.test(value)) return parseInt(value);
        if (value == "true" || value == "false") return value == "true";
        return value;
    }

    /**
     * SAML: the router is the Service Provider. It runs shibd, and once the login has
     * succeeded it proxies /auth/saml/login, with the attributes as headers, to a second
     * listener here that is not published and has no other routes. Putting the hand-off
     * on the API port would let anyone who can reach /jsonapi/* send forged headers.
     */
    setupSaml() {
        if (!this.allowedProviders.includes("saml")) return;
        this.handoffSecret = process.env.SEAD_SP_HANDOFF_SECRET || "";
        if (!this.handoffSecret) {
            console.warn('⚠️  SAML login not configured. Missing SEAD_SP_HANDOFF_SECRET.');
            return;
        }

        const handoffApp = express();
        this.installSessionSupport(handoffApp);

        handoffApp.get('/auth/saml/login', (req, res) => {
            if (!this.isValidHandoffSecret(req.get(HANDOFF_SECRET_HEADER))) {
                console.warn("SAML hand-off refused: missing or wrong hand-off secret");
                return res.status(403).send("Forbidden\n");
            }
            const user = samlUserFromHeaders(req.headers);
            if (!user) {
                console.warn("SAML hand-off refused: the IdP released neither subject-id nor eppn");
                return this.sendLoginFailure(res, "saml", "Your organisation did not release an identifier SEAD can use to sign you in.");
            }
            this.completeLogin(req, res, user, safeReturnPath(req.query.return));
        });

        handoffApp.use((req, res) => res.status(404).send("Not found\n"));

        const port = parseInt(process.env.HANDOFF_PORT) || 8485;
        this.handoffServer = http.createServer(handoffApp);
        this.handoffServer.listen(port, () => console.log("SAML hand-off listener started, listening at port", port));

        this.enabledProviders.saml = true;
        this.providers.push({
            id: "saml",
            label: process.env.SAML_LOGIN_LABEL || "SEAD login",
            loginUrl: "/auth/saml/login"
        });
    }

    isValidHandoffSecret(value) {
        if (typeof value !== "string") return false;
        const given = Buffer.from(value);
        const expected = Buffer.from(this.handoffSecret);
        return given.length === expected.length && crypto.timingSafeEqual(given, expected);
    }

    /**
     * ORCID over OpenID Connect. The id_token is signed and validated; its sub is the
     * ORCID iD. state and PKCE are handled by openid-client. ORCID_ISSUER picks the
     * sandbox (https://sandbox.orcid.org) or production (https://orcid.org).
     */
    setupOrcid() {
        if (!this.allowedProviders.includes("orcid")) return;
        if (!process.env.ORCID_CLIENT_ID || !process.env.ORCID_CLIENT_SECRET) {
            console.warn('⚠️  ORCID OAuth not configured. Missing ORCID_CLIENT_ID or ORCID_CLIENT_SECRET.');
            return;
        }
        const issuer = process.env.ORCID_ISSUER || "https://orcid.org";

        //Discovery needs ORCID to be reachable. It is done on the first login rather than
        //at startup, and retried on the next login if it fails, so an ORCID outage never
        //keeps the API from starting.
        let ready = null;
        const ensureStrategy = () => {
            if (!ready) {
                ready = oidc.discovery(new URL(issuer), process.env.ORCID_CLIENT_ID, process.env.ORCID_CLIENT_SECRET)
                    .then(config => {
                        passport.use(new OidcStrategy({
                            name: "orcid",
                            config,
                            scope: "openid",
                            callbackURL: this.publicOrigin+"/jsonapi/auth/orcid/callback"
                        }, (tokens, verified) => {
                            verified(null, orcidUserFromClaims(tokens.claims()));
                        }));
                    })
                    .catch(err => {
                        ready = null;
                        throw err;
                    });
            }
            return ready;
        };

        this.app.expressApp.get('/auth/orcid', async (req, res, next) => {
            try {
                await ensureStrategy();
            }
            catch (err) {
                console.error("ORCID discovery failed:", err.message);
                return this.sendLoginFailure(res, "orcid", "ORCID could not be reached. Please try again later.");
            }
            this.rememberReturnPath(req);
            passport.authenticate('orcid')(req, res, next);
        });

        this.app.expressApp.get('/auth/orcid/callback', async (req, res, next) => {
            try {
                await ensureStrategy();
            }
            catch (err) {
                console.error("ORCID discovery failed:", err.message);
                return this.sendLoginFailure(res, "orcid", "ORCID could not be reached. Please try again later.");
            }
            this.handleOAuthCallback('orcid', req, res, next);
        });

        this.enabledProviders.orcid = true;
        this.providers.push({ id: "orcid", label: "ORCID", loginUrl: "/jsonapi/auth/orcid" });
    }

    setupGoogle() {
        if (!this.allowedProviders.includes("google")) return;
        if (!process.env.GOOGLE_CLIENT_ID || !process.env.GOOGLE_CLIENT_SECRET) {
            console.warn('⚠️  Google OAuth not configured. Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET.');
            return;
        }
        passport.use(new GoogleStrategy({
            clientID: process.env.GOOGLE_CLIENT_ID,
            clientSecret: process.env.GOOGLE_CLIENT_SECRET,
            callbackURL: this.publicOrigin+'/jsonapi/auth/google/callback',
            state: true
        }, (accessToken, refreshToken, profile, done) => {
            const user = googleUserFromProfile(profile);
            return user ? done(null, user) : done(null, false, { message: "the Google account has no verified email" });
        }));
        this.registerOAuthRoutes('google', { scope: ['profile', 'email'] });
        this.enabledProviders.google = true;
        this.providers.push({ id: "google", label: "Google", loginUrl: "/jsonapi/auth/google" });
    }

    setupGitHub() {
        if (!this.allowedProviders.includes("github")) return;
        if (!process.env.GITHUB_CLIENT_ID || !process.env.GITHUB_CLIENT_SECRET) {
            console.warn('⚠️  GitHub OAuth not configured. Missing GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET.');
            return;
        }
        passport.use(new GitHubStrategy({
            clientID: process.env.GITHUB_CLIENT_ID,
            clientSecret: process.env.GITHUB_CLIENT_SECRET,
            callbackURL: this.publicOrigin+'/jsonapi/auth/github/callback',
            state: true
        }, (accessToken, refreshToken, profile, done) => {
            return done(null, profile);
        }));
        this.registerOAuthRoutes('github', { scope: ['user:email'] });
        this.enabledProviders.github = true;
        this.providers.push({ id: "github", label: "GitHub", loginUrl: "/jsonapi/auth/github" });
    }

    registerOAuthRoutes(provider, authenticateOptions) {
        this.app.expressApp.get('/auth/'+provider, (req, res, next) => {
            this.rememberReturnPath(req);
            passport.authenticate(provider, authenticateOptions)(req, res, next);
        });
        this.app.expressApp.get('/auth/'+provider+'/callback', (req, res, next) => {
            this.handleOAuthCallback(provider, req, res, next);
        });
    }

    /**
     * A login started with ?return=/path is a full-page login (the popup was blocked),
     * and ends with a redirect there. Without it, it ends with the popup page.
     */
    rememberReturnPath(req) {
        const returnPath = safeReturnPath(req.query.return);
        if (returnPath) {
            req.session.authReturnTo = returnPath;
        }
        else {
            delete req.session.authReturnTo;
        }
    }

    handleOAuthCallback(provider, req, res, next) {
        passport.authenticate(provider, (err, user, info) => {
            if (err || !user) {
                console.warn(`${provider} login failed:`, err ? err.message : (info && info.message) || "no user");
                return this.sendLoginFailure(res, provider, "Signing in with "+provider+" did not succeed.");
            }
            this.completeLogin(req, res, user, req.session.authReturnTo);
        })(req, res, next);
    }

    /**
     * Logs the user in (Passport regenerates the session, which sets the session cookie)
     * and ends the login: a redirect for a full-page login, otherwise the popup page.
     */
    completeLogin(req, res, user, returnPath) {
        req.login(user, async (err) => {
            if (err) {
                console.error("Login failed:", err);
                return this.sendLoginFailure(res, user.provider, "Signing in did not succeed.");
            }
            console.log(`User ${userIdOf(req.user)} signed in with ${user.provider}`);
            if (returnPath) {
                return res.redirect(returnPath);
            }
            const roles = await this.getRolesOrNone(req);
            this.sendPopupPage(res, { type: "login-success", provider: user.provider, user: req.user, roles });
        });
    }

    sendLoginFailure(res, provider, message) {
        this.sendPopupPage(res, { type: "login-failure", provider, message }, 401);
    }

    /**
     * The page the login popup ends on. It hands the result to the window that opened it
     * and closes. The result is embedded as JSON data with every character that could end
     * the script element escaped - a displayName from an IdP is not trusted markup - and
     * is posted only to our own origin.
     */
    sendPopupPage(res, message, status = 200) {
        const data = JSON.stringify({ message, origin: this.publicOrigin })
            .replace(/</g, "\\u003c")
            .replace(/>/g, "\\u003e")
            .replace(/&/g, "\\u0026")
            .replace(/\u2028/g, "\\u2028")
            .replace(/\u2029/g, "\\u2029");
        res.status(status);
        res.set("Cache-Control", "no-store");
        res.type("html");
        res.send(`<!doctype html>
<html>
<head><meta charset="utf-8"><title>SEAD sign in</title></head>
<body>
<script id="login-result" type="application/json">${data}</script>
<script>
(function() {
    var result = JSON.parse(document.getElementById("login-result").textContent);
    if (window.opener) {
        window.opener.postMessage(result.message, result.origin);
        window.close();
    }
    else {
        window.location.replace("/");
    }
})();
</script>
</body>
</html>`);
    }

    /**
     * For state-changing endpoints that act on the session. SameSite=Lax does not stop a
     * request that starts on super.sead.se from carrying the browser.sead.se cookie (they
     * are the same site), so the Origin has to be ours.
     */
    requireSameOrigin(req, res, next) {
        if (req.get("origin") !== this.publicOrigin) {
            console.warn(`Refused ${req.method} ${req.path} from origin ${req.get("origin") || "(none)"}`);
            return res.status(403).json({ error: "Cross-origin request refused" });
        }
        next();
    }

    /**
     * For endpoints that need a role: a signed-in user who has it, or the protected-endpoint
     * password (basic auth), which scripts use. A request that carries an Authorization
     * header is judged on the password alone. A session request has to come from our own
     * origin, like the other session endpoints. Answers 401 when neither is there - without
     * WWW-Authenticate, so a browser whose session has expired shows no password prompt.
     */
    requireRoleOrBasicAuth(role) {
        return async (req, res, next) => {
            if (req.get("authorization")) {
                return this.app.checkBasicAuth(req, res, next);
            }
            const userId = this.getUserId(req);
            if (!userId) {
                return res.status(401).json({ error: "Please sign in to do this.", code: "sign_in_required" });
            }
            let roles;
            try {
                roles = await this.userRoles.rolesOf(userId);
            }
            catch (err) {
                console.error(`Could not read the roles of ${userId}:`, err.message);
                return res.status(503).json({ error: "Your permissions could not be checked. Please try again later.", code: "roles_unavailable" });
            }
            if (!roles.includes(role)) {
                console.warn(`Refused ${req.method} ${req.path} to ${userId}: not ${role}`);
                return res.status(403).json({ error: "Your account does not have permission to do this.", code: "forbidden" });
            }
            this.requireSameOrigin(req, res, next);
        };
    }

    /** The signed-in user's roles; none when signed out, or when they cannot be read. */
    async getRolesOrNone(req) {
        const userId = this.getUserId(req);
        try {
            return await this.userRoles.rolesOf(userId);
        }
        catch (err) {
            console.error(`Could not read the roles of ${userId}:`, err.message);
            return [];
        }
    }

    generateRandomSecret() {
        return crypto.randomBytes(32).toString("hex");
    }

    close() {
        if (!this.handoffServer) return Promise.resolve();
        return new Promise(resolve => this.handoffServer.close(() => {
            console.log('SAML hand-off listener closed');
            resolve();
        }));
    }

    isAuthenticated(req) {
        return req.isAuthenticated();
    }

    getUser(req) {
        if (this.isAuthenticated(req)) {
            return req.user;
        }
        return null;
    }

    getUserId(req) {
        return userIdOf(this.getUser(req));
    }
}
