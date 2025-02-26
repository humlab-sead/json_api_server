
class Chronology {
    constructor(app, datingModule) {
        this.app = app;
        this.datingModule = datingModule;

        this.setupEndpoints();
    }

    setupEndpoints() {
        this.app.expressApp.post('/time/sites', async (req, res) => {
            let siteIds = req.body;
            if(typeof siteIds != "object") {
                res.status(400);
                res.send("Bad input - should be an array of site IDs");
                return;
            }
            siteIds.forEach(siteId => {
                if(!parseInt(siteId)) {
                    res.status(400);
                    res.send("Bad input - should be an array of site IDs");
                    return;
                }
            });

            let sitesWithTimeData = [];
            for(let key in siteIds) {
                let site = await this.app.getSite(siteIds[key]);
                sitesWithTimeData.push({
                    site_id: site.site_id,
                    hello: "yes, hello",
                    chronology_extremes: site.chronology_extremes
                });
                //sitesWithTimeData.push(await this.datingModule.fetchSiteTimeData(site));
            }
            
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(sitesWithTimeData, null, 2));
        });

        this.app.expressApp.get('/time/sites/build', async (req, res) => {
            this.buildSiteTimeData().then(data => {
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(data, null, 2));
            });
        });

        this.app.expressApp.get('/time/site/:siteId', async (req, res) => {
            let data = {};
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(data, null, 2));
        });

        this.app.expressApp.get('/time/sample/:sampleId', async (req, res) => {
            let data = {};
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(data, null, 2));
        });
    }

    /*
    async fetchSiteTime(siteId) {
        //results_chronology_temp
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sitesResult = await pgClient.query("SELECT * FROM results_chronology_temp ORDER BY site_id");
    }
    */
}

export default Chronology;