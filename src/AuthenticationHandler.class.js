import session from "express-session";
import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import { Strategy as GitHubStrategy } from "passport-github2";
import { Strategy as OrcidStrategy } from "passport-orcid";


export default class AuthenticationHandler {
    constructor(app) {
        this.app = app;

        // Initialize session
        this.app.expressApp.use(session({
            secret: process.env.SESSION_SECRET,
            resave: false,
            saveUninitialized: true,
            cookie: { secure: false } // Set to true if using HTTPS
        }));

        // Initialize Passport
        this.app.expressApp.use(passport.initialize());
        this.app.expressApp.use(passport.session());

        // Configure Google Strategy
        passport.use(new GoogleStrategy({
            clientID: process.env.GOOGLE_CLIENT_ID,
            clientSecret: process.env.GOOGLE_CLIENT_SECRET,
            callbackURL: 'http://localhost:8080/jsonapi/auth/google/callback'
        }, (accessToken, refreshToken, profile, done) => {
            return done(null, profile);
        }));

        // Configure GitHub Strategy
        passport.use(new GitHubStrategy({
            clientID: process.env.GITHUB_CLIENT_ID,
            clientSecret: process.env.GITHUB_CLIENT_SECRET,
            callbackURL: 'http://localhost:8080/jsonapi/auth/github/callback'
        }, (accessToken, refreshToken, profile, done) => {
            return done(null, profile);
        }));

        passport.use(new OrcidStrategy({
            clientID: process.env.ORCID_CLIENT_ID,
            clientSecret: process.env.ORCID_CLIENT_SECRET,
            callbackURL: 'http://localhost:8080/jsonapi/auth/orcid/callback',
            scope: '/authenticate'
        }, (accessToken, refreshToken, params, profile, done) => {
            // profile contains ORCID user info
            return done(null, profile);
        }));

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
                res.json({ loggedIn: false });
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