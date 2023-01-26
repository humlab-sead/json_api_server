
class SiteTime {
    constructor(app, datingMOdule) {
        this.app = app;
        this.datingModule = datingMOdule;

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

            let sitesWithTimeData = await this.fetchSiteTimeData(siteIds);
            res.header("Content-type", "application/json");
            res.send(JSON.stringify(sitesWithTimeData, null, 2));
        });

        this.app.expressApp.get('/time/sites/build', async (req, res) => {
            this.buildSiteTimeData().then(data => {
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(data, null, 2));
            });
        });

    }

    fetchSiteTimeData(siteIds) {

    }

    async buildSiteTimeData() {
        let pgClient = await this.app.getDbConnection();
        if(!pgClient) {
            return false;
        }

        let sites = [];
        let sitesResult = await pgClient.query("SELECT * FROM tbl_sites ORDER BY site_id");
        sitesResult.rows.forEach(row => {
            sites.push({
                siteId: parseInt(row.site_id)
            });
        })

        sites.forEach(siteDatingObject => {
            this.app.getObjectFromCache("sites", { site_id: siteDatingObject.siteId }).then(site => {

                if(site == false) {
                    console.log("No cached object for site", siteDatingObject.siteId);
                }
                else {
                    if(typeof site.data_groups != "undefined") {
                        site.data_groups.forEach(dataGroup => {
                            if(dataGroup.type == "dating_values") {
                                console.log("site "+site.site_id);
                                let ages = this.datingModule.getNormalizedDatingSpanFromDataGroup(dataGroup);
                                console.log(ages);
                            }
                        });
                    }
                    else {
                        console.log("site", siteDatingObject.siteId, "has no data_groups");
                    }
                }

                
                
                
            });
        });
        
        
        this.app.releaseDbConnection(pgClient);
        return sites;
    }
}

module.exports = SiteTime;