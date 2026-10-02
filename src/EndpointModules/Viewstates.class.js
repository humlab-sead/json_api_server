import crypto from 'crypto';

class Viewstates {
    constructor(app) {
        this.app = app;

        if(!process.env.JAS_AUTH_SALT) {
            console.warn('⚠️  JAS_AUTH_SALT not set. Viewstate owners are pseudonymised without a salt.');
        }

        const sameOrigin = this.app.authHandler.requireSameOrigin.bind(this.app.authHandler);

        //Who a viewstate belongs to comes from the session. The :userIdToken the client
        //used to send is accepted and ignored, so an older client keeps working.
        this.app.expressApp.get('/viewstates', this.handleViewStateListGet.bind(this));
        this.app.expressApp.get('/viewstates/:userIdToken', this.handleViewStateListGet.bind(this));
        this.app.expressApp.get('/viewstate/:viewstateId', this.handleViewStateGet.bind(this));
        this.app.expressApp.post('/viewstate', sameOrigin, this.handleViewStatePost.bind(this));
        //We never actually delete anything, but we use this for removing the associated user information
        this.app.expressApp.delete('/viewstate/:viewstateId', sameOrigin, this.handleViewstateDelete.bind(this));
        this.app.expressApp.delete('/viewstate/:viewstateId/:userIdToken', sameOrigin, this.handleViewstateDelete.bind(this));
    }

    /**
     * What a viewstate's owner is stored as: a salted hash of the user id, so the
     * stored documents hold no email address or institutional identifier.
     */
    getUserToken(userId) {
        let shaHasher = crypto.createHash('sha1');
        const salt = process.env.JAS_AUTH_SALT;
        shaHasher.update(userId+salt);
        let userToken = shaHasher.digest('hex');
        return userToken;
    }

    getRequestUserToken(req) {
        const userId = this.app.authHandler.getUserId(req);
        return userId ? this.getUserToken(userId) : null;
    }

    async handleViewStateGet(req, res) {
        try {
            //Viewstates are public; who saved one is not
            const viewStates = await this.getViewState(req.params.viewstateId).project({ user: 0 }).toArray();
            return res.send(viewStates);
        }
        catch(err) {
            console.error("Could not fetch viewstate", req.params.viewstateId, err);
            return res.status(500).send('{"status": "failed"}');
        }
    }

    async handleViewStateListGet(req, res) {
        const userToken = this.getRequestUserToken(req);
        if(!userToken) {
            return res.status(401).send('{"status": "failed"}');
        }

        try {
            const viewStates = await this.getViewStateList(userToken).project({ user: 0 }).toArray();
            return res.send(viewStates);
        }
        catch(err) {
            console.error("Could not list viewstates", err);
            return res.status(500).send('{"status": "failed"}');
        }
    }

    async handleViewstateDelete(req, res) {
        const userToken = this.getRequestUserToken(req);
        if(!userToken) {
            return res.status(401).send('{"status": "failed"}');
        }

        //Only the owner can detach themselves from a viewstate
        const result = await this.deleteUserFromViewstate(req.params.viewstateId, userToken);
        if(result.matchedCount == 0) {
            return res.status(404).send('{"status": "failed"}');
        }
        console.log("Anonymized viewstate "+req.params.viewstateId);
        return res.send('{"status": "ok"}');
    }

    async handleViewStatePost(req, res) {
        const userToken = this.getRequestUserToken(req);
        if(!userToken) {
            return res.status(401).send('{"status": "failed"}');
        }

        let viewState;
        try {
            viewState = JSON.parse(req.body.data);
        }
        catch(err) {
            return res.status(400).send('{"status": "failed"}');
        }
        if(!viewState || typeof viewState.id != "string" || viewState.id.length == 0) {
            return res.status(400).send('{"status": "failed"}');
        }

        await this.saveViewState(userToken, viewState);
        console.log("Stored viewstate "+viewState.id);
        return res.send('{"status": "ok"}');
    }


    // low level operations
    getViewStateList(userToken) {
        return this.app.mongo.collection('viewstates').find({ user: userToken });
    }

    getViewState(vsId) {
        return this.app.mongo.collection('viewstates').find({ id: vsId });
    }

    async saveViewState(userToken, viewState) {
        viewState.user = userToken;
        await this.app.mongo.collection('viewstates').insertOne(viewState);
        return true;
    }

    deleteUserFromViewstate(viewstateId, userToken) {
        return this.app.mongo.collection('viewstates').updateOne({ id: viewstateId, user: userToken }, { $set: { user: "deleted" } });
    }

    deleteViewstate(viewstateId) {
        return this.app.mongo.collection('viewstates').deleteOne({ id: viewstateId });
    }
}

export default Viewstates;
