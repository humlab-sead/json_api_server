import crypto from 'crypto';

class Viewstates {
    constructor(app) {
        this.app = app;

        /* not sure this should be here - it's in the original code, but that was it's own standalone server
        app.use(bodyParser.json());
        app.use(function(req, res, next) {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
            res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE");
            next();
        });
        */

        this.app.expressApp.get('/viewstates/:userIdToken', (req, res) => { this.handleViewStateListGet(req, res); });
        this.app.expressApp.get('/viewstate/:viewstateId', this.handleViewStateGet.bind(this));
        this.app.expressApp.post('/viewstate', this.handleViewStatePost.bind(this));
        this.app.expressApp.delete('/viewstate/:viewstateId/:userIdToken', this.handleViewstateDelete.bind(this)); //We never actually delete anything, but we use this for removing the associated user information
    }

    getUserToken(userEmail) {
	    console.log("getUserToken");
        let shaHasher = crypto.createHash('sha1');
        const salt = process.env.JAS_AUTH_SALT;
        shaHasher.update(userEmail+salt);
        let userToken = shaHasher.digest('hex');
        return userToken;
    }
    
    handleNullRequest(req, res) {
	    console.log("handleNullRequest");
        return res.send("No such command");
    }
    
    handleViewStateGet(req, res) {
	    console.log("handleViewStateGet");
        const viewStatesCur = this.getViewState(req.params.viewstateId);
        let viewStates = [];
        viewStatesCur.toArray((err, viewStates) => {
            if (err) throw err;
            return res.send(viewStates);
        });
    }

    handleViewStateListGet(req, res) {
	    console.log("handleViewStateListGet");

        let userId = this.app.authHandler.getUserId(req);

        if(!userId) {
            console.log("FAILED sending list of viewstates for user");
            return res.send('{"status": "failed"}');
        }

        const viewStatesCur = this.getViewStateList(userId);
            
        viewStatesCur.toArray((err, viewStates) => {
            if (err) throw err;
            console.log("Sending list of viewstates for user "+userId);
            return res.send(viewStates);
        });
    }

    handleViewstateDelete(req, res) {
        let user = this.app.authHandler.getUser(req);
        if(!user) {
            console.log("handleViewstateDelete - no user");
            return res.send('{"status": "failed"}');
        }

	    console.log("handleViewstateDelete");
        const viewStatesCur = this.getViewState(req.params.viewstateId);
        viewStatesCur.toArray((err, viewStates) => {
            if (err) throw err;
            console.log("Request to anonymize viewstate "+req.params.viewstateId);

            const user = new User();
            let userVerifyPromise = user.verifyGoogleUser(req.params.userIdToken);
            userVerifyPromise.then(userObj => {
                if(userObj === false) {
                    console.log("FAILED anonymizing viewstate "+req.params.viewstateId+", userObj is false ");
                    return res.send('{"status": "failed"}');
                }

                this.deleteUserFromViewstate(req.params.viewstateId);

                console.log("Anonymized viewstate "+req.params.viewstateId+" which belonged to user "+user.getUserToken());
                return res.send('{"status": "ok"}');
            });
        });
    }
    
    async handleViewStatePost(req, res) {
	    console.log("handleViewStatePost");
        let jsonData = req.body;
        const user = this.app.authHandler.getUser(req);
        if(!user) {
            console.log("handleViewStatePost - no user");
            return res.send('{"status": "failed"}');
        }

        console.log(JSON.stringify(user, null, 2));

        let userId = this.app.authHandler.getUserId(req);
        let status = await this.saveViewState(userId, jsonData.data);
        if(!status) {
            return res.send('{"status": "failed"}');
        }
        console.log("Storing viewstate for user "+userId);
        return res.send('{"status": "ok"}');
    }


    // low level operations
    getViewStateList(userId) {
        const cursor = this.app.mongo.collection('viewstates').find({ user: userId });
        return cursor;
    }

    getViewState(vsId) {
        const cursor = this.app.mongo.collection('viewstates').find({ id: vsId });
        return cursor;
    }

    loadViewState(viewStateId) {
        const cursor = this.app.mongo.collection('viewstates').find({ id: viewStateId });
    }
      
    async saveViewState(user, viewState) {
        viewState = JSON.parse(viewState);

        viewState.user = user.email;

        await this.app.mongo.collection('viewstates').insertOne(viewState);

        console.log("Viewstate inserted");
        return true;
    }

    deleteUserFromViewstate(viewstateId) {
        const cursor = this.app.mongo.collection('viewstates').update({ id: viewstateId }, { user: "deleted" });
        return cursor;
    }

    deleteViewstate(viewstateId) {
        const cursor = this.app.mongo.collection('viewstates').deleteOne({ id: viewstateId });
        return cursor;
    }
}

class User {
    constructor() {
    }

    async verifyGoogleUser(token) {
        let googleClientId = process.env.JAS_GOOGLE_CLIENT_ID;
        if(!googleClientId) {
            console.log("No google client id set");
            return false;
        }
        
        const client = new OAuth2Client(googleClientId);
        const ticket = await client.verifyIdToken({
            idToken: token,
            audience: googleClientId
        });
        const payload = ticket.getPayload();
        const userid = payload['sub'];

        let userValid = true;
        let error = "";

        if(payload.aud != googleClientId) {
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
        const salt = process.env.JAS_AUTH_SALT;
        shaHasher.update(this.userObj.email+salt);
        let userToken = shaHasher.digest('hex');
        return userToken;
    }
}

export default Viewstates;