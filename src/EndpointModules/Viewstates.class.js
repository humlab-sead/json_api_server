import crypto from 'crypto';

/*
* Viewstates are saved views of the client: its filters, result view and layout.
*
* Each is public or private, as its owner chose when saving it (visibility). A public one
* can be opened by anyone with its link; a private one only by its owner, and to anyone
* else it is as though it did not exist. One saved before there was a choice has no
* visibility, and is public, as all of them were then. The owner can change it later.
*
* The owner is stored as a pseudonym (getUserToken). Detaching yourself from a public
* viewstate leaves it, unowned, for its link; a private one nobody else could open is
* deleted instead. Deleting an account (forgetUser) does the same with all of them.
*/
export const VISIBILITIES = ["public", "private"];

/** A viewstate's visibility: what it was saved with, public if nothing. */
export function visibilityOf(viewState) {
    return viewState && viewState.visibility === "private" ? "private" : "public";
}

/** Whether the user (by token, null when signed out) may open the viewstate. */
export function mayOpen(viewState, userToken) {
    return visibilityOf(viewState) === "public" || (userToken != null && viewState.user === userToken);
}

class Viewstates {
    constructor(app) {
        this.app = app;

        if(!process.env.JAS_AUTH_SALT) {
            console.warn('⚠️  JAS_AUTH_SALT not set. Viewstate owners are pseudonymised without a salt.');
        }

        const sameOrigin = this.app.authHandler.requireSameOrigin.bind(this.app.authHandler);
        //Keeping viewstates under a user is part of their account, which needs the privacy
        //policy accepted. Detaching yourself from one does not.
        const consented = this.app.authHandler.requireConsent.bind(this.app.authHandler);

        //Who a viewstate belongs to comes from the session. The :userIdToken the client
        //used to send is accepted and ignored, so an older client keeps working.
        this.app.expressApp.get('/viewstates', consented, this.handleViewStateListGet.bind(this));
        this.app.expressApp.get('/viewstates/:userIdToken', consented, this.handleViewStateListGet.bind(this));
        this.app.expressApp.get('/viewstate/:viewstateId', this.handleViewStateGet.bind(this));
        this.app.expressApp.post('/viewstate', sameOrigin, consented, this.handleViewStatePost.bind(this));
        this.app.expressApp.patch('/viewstate/:viewstateId', sameOrigin, consented, this.handleViewStatePatch.bind(this));
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
            //A private viewstate is answered like one that does not exist, unless it is yours.
            //Who saved one is never told.
            const userToken = this.getRequestUserToken(req);
            res.set("Cache-Control", "no-store");
            const viewStates = (await this.getViewState(req.params.viewstateId).toArray())
                .filter(viewState => mayOpen(viewState, userToken))
                .map(({ user, ...viewState }) => ({ ...viewState, visibility: visibilityOf(viewState), yours: userToken != null && user === userToken }));
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
            const viewStates = (await this.getViewStateList(userToken).project({ user: 0 }).toArray())
                .map(viewState => ({ ...viewState, visibility: visibilityOf(viewState) }));
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

        //Only the owner can detach themselves from a viewstate. A private one would be left
        //where nobody could open it, so it goes altogether.
        const deleted = await this.app.mongo.collection('viewstates').deleteOne({ id: req.params.viewstateId, user: userToken, visibility: "private" });
        if(deleted.deletedCount > 0) {
            console.log("Deleted private viewstate "+req.params.viewstateId);
            return res.send('{"status": "ok"}');
        }
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
        //Public unless asked otherwise; a client from before there was a choice does not ask
        const visibility = req.body.visibility === undefined ? "public" : req.body.visibility;
        if(!VISIBILITIES.includes(visibility)) {
            return res.status(400).send('{"status": "failed"}');
        }
        viewState.visibility = visibility;

        await this.saveViewState(userToken, viewState);
        console.log("Stored viewstate "+viewState.id);
        return res.send('{"status": "ok"}');
    }


    /*
    * Makes one of your viewstates public or private: { visibility: "public" | "private" }.
    */
    async handleViewStatePatch(req, res) {
        const userToken = this.getRequestUserToken(req);
        const visibility = req.body ? req.body.visibility : null;
        if(!VISIBILITIES.includes(visibility)) {
            return res.status(400).json({ status: "failed", error: "'visibility' must be \"public\" or \"private\"." });
        }
        try {
            const result = await this.app.mongo.collection('viewstates').updateOne({ id: req.params.viewstateId, user: userToken }, { $set: { visibility } });
            if(result.matchedCount == 0) {
                return res.status(404).json({ status: "failed", error: "You have no viewstate with that id." });
            }
            console.log("Viewstate "+req.params.viewstateId+" is "+visibility+" now");
            return res.json({ status: "ok", id: req.params.viewstateId, visibility });
        }
        catch(err) {
            console.error("Could not change viewstate", req.params.viewstateId, err);
            return res.status(500).json({ status: "failed" });
        }
    }

    /** How many viewstates the user has. */
    countOf(userId) {
        return this.app.mongo.collection('viewstates').countDocuments({ user: this.getUserToken(userId) });
    }

    /*
    * For a deleted account: its private viewstates are deleted, and its public ones are kept
    * for their links but no longer linked to it.
    */
    async forgetUser(userId) {
        const collection = this.app.mongo.collection('viewstates');
        const userToken = this.getUserToken(userId);
        await collection.deleteMany({ user: userToken, visibility: "private" });
        await collection.updateMany({ user: userToken }, { $set: { user: "deleted" } });
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
