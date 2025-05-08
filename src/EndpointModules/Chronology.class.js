
class Chronology {
    constructor(app, datingModule) {
        this.app = app;
        this.datingModule = datingModule;

        this.setupEndpoints();
    }

    validateAndSanitizeSiteIds(rawSiteIds) {
        if (!Array.isArray(rawSiteIds)) {
            return { valid: false, sanitized: [], error: "'siteIds' must be an array." };
        }
    
        const sanitized = rawSiteIds
            .map(id => parseInt(id))
            .filter(id => !isNaN(id) && id > 0); // Only positive integers
    
        if (sanitized.length !== rawSiteIds.length) {
            return { valid: false, sanitized: [], error: "All 'siteIds' must be valid positive integers." };
        }
    
        return { valid: true, sanitized };
    }

    setupEndpoints() {
        this.app.expressApp.post('/time/sites', async (req, res) => {
            try {
                const { valid, sanitized, error } = this.validateAndSanitizeSiteIds(req.body.siteIds);
        
                if (!valid) {
                    return res.status(400).json({ error: "Bad input - " + error });
                }
        
                const sitesWithTimeData = [];
        
                for (const id of sanitized) {
                    try {
                        const site = await this.app.getSite(id);
                        if (!site) {
                            console.warn(`No site found for ID ${id}`);
                            continue;
                        }
        
                        const timeData = await this.datingModule.fetchSiteTimeData(site);
                        sitesWithTimeData.push(timeData);
                    } catch (innerErr) {
                        console.error(`Error processing site ID ${id}:`, innerErr);
                        continue;
                    }
                }
        
                res.status(200).json(sitesWithTimeData);
        
            } catch (err) {
                console.error("Unhandled error in /time/sites:", err);
                res.status(500).json({ error: "Internal server error." });
            }
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