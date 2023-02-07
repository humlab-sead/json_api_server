const crypto = require('crypto');
const { OAuth2Client } = require('google-auth-library');

class ViewstateManager {
    constructor(app) {
        this.app = app;
        this.setupEndpoints(app);
    }

    setupEndpoints(app) {
        app.get('/', this.handleNullRequest);
        app.get('/viewstates/:userIdToken', this.handleViewStateListGet);
        app.get('/viewstate/:viewstateId', this.handleViewStateGet);
        app.post('/viewstate', this.handleViewStatePost);
        app.delete('/viewstate/:viewstateId/:userIdToken', this.handleViewstateDelete); //We never actually delete anything, but we use this for removing the associated user information
    }

    getUserToken(userEmail) {
	    console.log("getUserToken");
        let shaHasher = crypto.createHash('sha1');
        shaHasher.update(userEmail+this.app.passwordSalt);
        let userToken = shaHasher.digest('hex');
        return userToken;
    }
    
    handleNullRequest(req, res) {
	    console.log("handleNullRequest");
        return res.send("No such command");
    }
    
    async handleViewStateGet(req, res) {
	    console.log("handleViewStateGet");
        let findResult = await this.app.mongo.collection("viewstate").find({ id: req.params.viewstateId }).toArray();
        res.send(findResult);
    }

    async handleViewstateDelete(req, res) {
	    console.log("handleViewstateDelete");
        let viewStates = await this.app.mongo.collection("viewstate").find({ id: req.params.viewstateId }).toArray();
        userEmail = viewStates[0].user;
        console.log("Request to anonymize viewstate "+req.params.viewstateId);

        const user = new User(this.app);
        let userVerifyPromise = user.verifyGoogleUser(req.params.userIdToken);
        userVerifyPromise.then(async userObj => {
            if(userObj === false) {
                console.log("FAILED anonymizing viewstate "+req.params.viewstateId+", userObj is false ");
                return res.send('{"status": "failed"}');
            }

            await this.app.mongo.collection("viewstate").update({ id: req.params.viewstateId }, { user: "deleted" });

            console.log("Anonymized viewstate "+req.params.viewstateId+" which belonged to user "+user.getUserToken());
            return res.send('{"status": "ok"}');
        });
    }

    async handleViewStateListGet(req, res) {
	    console.log("handleViewStateListGet");
        const user = new User();
        let userVerifyPromise = user.verifyGoogleUser(req.params.userIdToken);
	    console.log("User verification pending");
        userVerifyPromise.then(async userObj => {
            if(userObj === false) {
                console.log("FAILED sending list of viewstates for user "+this.getUserToken(user.email));
                return res.send('{"status": "failed"}');
            }

            await this.app.mongo.collection("viewstate").insertOne(viewState);

            new Database(global.config).connect().then(db => {

                const viewStatesCur = this.db.collection('viewstate').find({ user: user.getUserToken() });
                
                let viewStates = [];
                viewStatesCur.toArray((err, viewStates) => {
                    if (err) throw err;
                    db.disconnect();
                    console.log("Sending list of viewstates for user "+user.getUserToken());
                    return res.send(viewStates);
                  });
              }).catch(() => { console.log("Everything has gone to hell"); });
        });
    }
    
    async handleViewStatePost(req, res) {
	    console.log("handleViewStatePost");
        let jsonData = req.body;
        const user = new User();
        let userVerifyPromise = user.verifyGoogleUser(jsonData.user_id_token);
        
        userVerifyPromise.then(async userObj => {
            if(userObj === false) {
                console.log("FAILED storing viewstate - no user");
                return res.send('{"status": "failed"}');
            }

            let viewState = jsonData.data;
            viewState.user = user.getUserToken();

            await this.app.mongo.collection("viewstate").insertOne(viewState);
            return res.send('{"status": "ok"}');
        });
    }
}

class User {
    constructor(app) {
        this.app = app;
    }

    async verifyGoogleUser(token) {
        const client = new OAuth2Client(config.google.client_id);
        const ticket = await client.verifyIdToken({
            idToken: token,
            audience: global.config.google.client_id
        });
        const payload = ticket.getPayload();
        const userid = payload['sub'];

        let userValid = true;
        let error = "";

        if(payload.aud != global.config.google.client_id) {
            userValid = false;
            error = "Payload aud doesn't equal google client id";
        }
        if(payload.iss != "accounts.google.com" && payload.iss != "https://accounts.google.com") {
            userValid = false;  
            error = "Payload iss doesn't equal expected domains";
        }

        let unixTime = Math.round(Date.now()/1000);
        if(payload.exp < unixTime) {
            userValid = false;
            error = "Ticket has expired";
        }

        if(userValid) {
            this.userObj = payload;
            return payload;
        }
        return false;
    }

    getUserToken() {
        let shaHasher = crypto.createHash('sha1');
        shaHasher.update(this.userObj.email+this.app.passwordSalt);
        let userToken = shaHasher.digest('hex');
        return userToken;
    }
}

module.exports = { ViewstateManager, User };
