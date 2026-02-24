import session from "express-session";
import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import { Strategy as GitHubStrategy } from "passport-github2";
import { Strategy as OrcidStrategy } from "passport-orcid";


export default class AuthenticationHandler {
    constructor(app) {
        this.app = app;
        this.enabledProviders = {
            google: false,
            github: false,
            orcid: false
        };

        // Check for session secret
        const sessionSecret = process.env.SESSION_SECRET || this.generateRandomSecret();
        if (!process.env.SESSION_SECRET) {
            console.warn('⚠️  SESSION_SECRET not set. Using a random secret. Sessions will not persist across server restarts.');
        }

        // Initialize session
        this.app.expressApp.use(session({
            secret: sessionSecret,
            resave: false,
            saveUninitialized: true,
            cookie: { secure: false } // Set to true if using HTTPS
        }));

        // Initialize Passport
        this.app.expressApp.use(passport.initialize());
        this.app.expressApp.use(passport.session());

        // Configure Google Strategy (if credentials are available)
        if (process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET) {
            passport.use(new GoogleStrategy({
                clientID: process.env.GOOGLE_CLIENT_ID,
                clientSecret: process.env.GOOGLE_CLIENT_SECRET,
                callbackURL: 'http://localhost:8080/jsonapi/auth/google/callback'
            }, (accessToken, refreshToken, profile, done) => {
                return done(null, profile);
            }));
            this.enabledProviders.google = true;
        } else {
            console.warn('⚠️  Google OAuth not configured. Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET.');
        }

        // Configure GitHub Strategy (if credentials are available)
        if (process.env.GITHUB_CLIENT_ID && process.env.GITHUB_CLIENT_SECRET) {
            passport.use(new GitHubStrategy({
                clientID: process.env.GITHUB_CLIENT_ID,
                clientSecret: process.env.GITHUB_CLIENT_SECRET,
                callbackURL: 'http://localhost:8080/jsonapi/auth/github/callback'
            }, (accessToken, refreshToken, profile, done) => {
                return done(null, profile);
            }));
            this.enabledProviders.github = true;
        } else {
            console.warn('⚠️  GitHub OAuth not configured. Missing GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET.');
        }

        // Configure ORCID Strategy (if credentials are available)
        if (process.env.ORCID_CLIENT_ID && process.env.ORCID_CLIENT_SECRET) {
            passport.use(new OrcidStrategy({
                clientID: process.env.ORCID_CLIENT_ID,
                clientSecret: process.env.ORCID_CLIENT_SECRET,
                callbackURL: 'http://localhost:8080/jsonapi/auth/orcid/callback',
                scope: '/authenticate'
            }, (accessToken, refreshToken, params, profile, done) => {
                // profile contains ORCID user info
                return done(null, profile);
            }));
            this.enabledProviders.orcid = true;
        } else {
            console.warn('⚠️  ORCID OAuth not configured. Missing ORCID_CLIENT_ID or ORCID_CLIENT_SECRET.');
        }

        // Serialize user
        passport.serializeUser((user, done) => {
            done(null, user);
        });

        // Deserialize user
        passport.deserializeUser((user, done) => {
            done(null, user);
        });

        this.app.expressApp.get('/auth/status', (req, res) => {
            if (req.isAuthenticated()) {
                res.json({ loggedIn: true, user: req.user });
            } else {
                res.json({ loggedIn: false, enabledProviders: this.enabledProviders });
            }
        });

        this.app.expressApp.post('/auth/logout', (req, res) => {
            req.logout((err) => {
                if (err) {
                    return res.status(500).json({ error: 'Logout failed' });
                }
                res.json({ message: 'Logged out successfully' });
            });
        });

        // Google routes (only if configured)
        if (this.enabledProviders.google) {
            this.app.expressApp.get('/auth/google',
                passport.authenticate('google', { scope: ['profile', 'email'] })
            );

            this.app.expressApp.get('/auth/google/callback',
                passport.authenticate('google', { failureRedirect: '/' }),
                (req, res) => {
                    // Send a script that posts back to the opener and closes the popup
                    res.send(`
                    <script>
                        window.opener.postMessage(
                            { type: "login-success", provider: "google", user: ${JSON.stringify(req.user)} },
                            window.location.origin
                        );
                        window.close();
                    </script>
                    `);
                }
            );
        }

        // GitHub routes (only if configured)
        if (this.enabledProviders.github) {
            this.app.expressApp.get('/auth/github',
                passport.authenticate('github', { scope: ['user:email'] })
            );

            this.app.expressApp.get('/auth/github/callback',
                passport.authenticate('github', { failureRedirect: '/' }),
                (req, res) => {
                    // Send a script that posts back to the opener and closes the popup
                    res.send(`
                    <script>
                        window.opener.postMessage(
                            { type: "login-success", provider: "github", user: ${JSON.stringify(req.user)} },
                            window.location.origin
                        );
                        window.close();
                    </script>
                    `);
                }
            );
        }

        // ORCID routes (only if configured)
        if (this.enabledProviders.orcid) {
            this.app.expressApp.get('/auth/orcid',
                passport.authenticate('orcid')
            );

            this.app.expressApp.get('/auth/orcid/callback',
                passport.authenticate('orcid', { failureRedirect: '/' }),
                (req, res) => {
                    res.send(`
                    <script>
                        window.opener.postMessage(
                            { type: "login-success", provider: "orcid", user: ${JSON.stringify(req.user)} },
                            window.location.origin
                        );
                        window.close();
                    </script>
                    `);
                }
            );
        }

        // Log authentication status
        const enabledCount = Object.values(this.enabledProviders).filter(v => v).length;
        if (enabledCount === 0) {
            console.warn('⚠️  No authentication providers configured. Server will run without authentication.');
        } else {
            const providers = Object.keys(this.enabledProviders).filter(k => this.enabledProviders[k]);
            console.log(`✓ Authentication enabled for: ${providers.join(', ')}`);
        }
    }

    generateRandomSecret() {
        return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
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
        const user = this.getUser(req);
        if (user) {
            return user.emails[0]+"-"+user.provider; // Use email and provider as unique identifier
        }
        return null;
    }
}