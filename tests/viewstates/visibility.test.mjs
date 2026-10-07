/**
 * Public and private viewstates (src/EndpointModules/Viewstates.class.js): a private one opens
 * for its owner only, and is answered like an unknown id to anyone else. Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import Viewstates, { visibilityOf, mayOpen } from "../../src/EndpointModules/Viewstates.class.js";

const OWNER = { provider: "orcid", id: "0000-0002-1825-0097" };
const OTHER = { provider: "saml", id: "other@umu.se" };

/** A viewstates collection over an array, with what the module uses. */
function viewstatesCollection(docs) {
    const matches = (doc, query) => Object.entries(query).every(([key, value]) => doc[key] === value);
    return {
        docs,
        find: query => {
            const found = docs.filter(doc => matches(doc, query));
            return {
                toArray: async () => found.map(doc => ({ ...doc })),
                project: projection => ({ toArray: async () => found.map(doc => {
                    const copy = { ...doc };
                    Object.keys(projection).forEach(key => delete copy[key]);
                    return copy;
                }) }),
            };
        },
        insertOne: async doc => docs.push(doc),
        updateOne: async (query, update) => {
            const doc = docs.find(d => matches(d, query));
            if (doc) Object.assign(doc, update.$set);
            return { matchedCount: doc ? 1 : 0 };
        },
        updateMany: async (query, update) => docs.filter(d => matches(d, query)).forEach(d => Object.assign(d, update.$set)),
        deleteOne: async query => {
            const index = docs.findIndex(d => matches(d, query));
            if (index >= 0) docs.splice(index, 1);
            return { deletedCount: index >= 0 ? 1 : 0 };
        },
        deleteMany: async query => {
            for (let i = docs.length - 1; i >= 0; i--) if (matches(docs[i], query)) docs.splice(i, 1);
        },
        countDocuments: async query => docs.filter(d => matches(d, query)).length,
    };
}

function viewstates(docs = []) {
    const collection = viewstatesCollection(docs);
    const routes = {};
    const route = method => (path, ...handlers) => { routes[method+" "+path] = handlers.at(-1); };
    const app = {
        mongo: { collection: () => collection },
        expressApp: { get: route("GET"), post: route("POST"), patch: route("PATCH"), delete: route("DELETE") },
        authHandler: {
            requireSameOrigin: (req, res, next) => next(),
            requireConsent: (req, res, next) => next(),
            getUserId: req => req.user ? `${req.user.provider}:${req.user.id}` : null,
        },
    };
    const module = new Viewstates(app);
    return { module, routes, collection };
}

function response() {
    return {
        statusCode: 200, body: null,
        status(code) { this.statusCode = code; return this; },
        send(body) { this.body = typeof body === "string" ? JSON.parse(body) : body; return this; },
        json(body) { this.body = body; return this; },
        set() {},
    };
}

async function call(handler, user, { params = {}, body } = {}) {
    const res = response();
    await handler({ user, params, body }, res);
    return res;
}

test("a viewstate without a visibility is public, as all of them were", () => {
    assert.equal(visibilityOf({}), "public");
    assert.equal(visibilityOf({ visibility: "private" }), "private");
    assert.equal(visibilityOf({ visibility: "nonsense" }), "public");
    assert.equal(mayOpen({ visibility: "private", user: "abc" }, null), false);
    assert.equal(mayOpen({ visibility: "private", user: "abc" }, "abc"), true);
    assert.equal(mayOpen({}, null), true);
});

test("saving: public unless private is asked for, and nothing else", async () => {
    const { routes, collection } = viewstates();
    const save = (id, visibility) => call(routes["POST /viewstate"], OWNER, { body: { data: JSON.stringify({ id }), visibility } });
    assert.equal((await save("a")).body.status, "ok");
    assert.equal((await save("b", "private")).body.status, "ok");
    assert.equal((await save("c", "secret")).statusCode, 400);
    assert.deepEqual(collection.docs.map(d => [d.id, d.visibility]), [["a", "public"], ["b", "private"]]);
});

test("a private viewstate opens for its owner only, and is unknown to anyone else", async () => {
    const { module, routes } = viewstates();
    const token = module.getUserToken("orcid:0000-0002-1825-0097");
    module.app.mongo.collection().docs.push({ id: "p", user: token, visibility: "private" }, { id: "o", user: token });
    const open = (id, user) => call(routes["GET /viewstate/:viewstateId"], user, { params: { viewstateId: id } });

    const own = (await open("p", OWNER)).body;
    assert.equal(own.length, 1);
    assert.equal(own[0].user, undefined, "who saved it is never told");
    assert.equal(own[0].yours, true);
    assert.deepEqual((await open("p", OTHER)).body, []);
    assert.deepEqual((await open("p", null)).body, []);

    const old = (await open("o", null)).body;
    assert.equal(old[0].visibility, "public");
    assert.equal(old[0].yours, false);
});

test("the owner can change the visibility; nobody else can", async () => {
    const { module, routes, collection } = viewstates();
    collection.docs.push({ id: "v", user: module.getUserToken("orcid:0000-0002-1825-0097"), visibility: "public" });
    const patch = (user, visibility) => call(routes["PATCH /viewstate/:viewstateId"], user, { params: { viewstateId: "v" }, body: { visibility } });
    assert.equal((await patch(OTHER, "private")).statusCode, 404);
    assert.equal((await patch(OWNER, "hidden")).statusCode, 400);
    assert.equal((await patch(OWNER, "private")).body.visibility, "private");
    assert.equal(collection.docs[0].visibility, "private");
});

test("detaching: a public viewstate stays for its link, a private one goes", async () => {
    const { module, routes, collection } = viewstates();
    const token = module.getUserToken("orcid:0000-0002-1825-0097");
    collection.docs.push({ id: "pub", user: token, visibility: "public" }, { id: "priv", user: token, visibility: "private" });
    const detach = id => call(routes["DELETE /viewstate/:viewstateId"], OWNER, { params: { viewstateId: id } });
    await detach("pub");
    await detach("priv");
    assert.deepEqual(collection.docs, [{ id: "pub", user: "deleted", visibility: "public" }]);
});

test("a deleted account's private viewstates are deleted, its public ones unlinked", async () => {
    const { module, collection } = viewstates();
    const token = module.getUserToken("orcid:0000-0002-1825-0097");
    collection.docs.push({ id: "pub", user: token }, { id: "priv", user: token, visibility: "private" }, { id: "theirs", user: "someone" });
    assert.equal(await module.countOf("orcid:0000-0002-1825-0097"), 2);
    await module.forgetUser("orcid:0000-0002-1825-0097");
    assert.deepEqual(collection.docs.map(d => [d.id, d.user]), [["pub", "deleted"], ["theirs", "someone"]]);
});
