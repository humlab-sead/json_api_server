/*
* Class: PageExists
* Whether a site report or taxon datasheet in the client is a page, so it can be a 404 if it is not.
*
* The client is a single page application, which would otherwise be served for any site or taxon id,
* to show a "not found" that only people see: search engines get a page for each, and a status 200.
* The router asks here before serving one, with nginx's auth_request, which only knows 2xx (go on)
* and 401/403 (stop) - anything else is an error - hence 403 for a page that is not there.
*/
class PageExists {
    //The ids are integers in postgres, a longer one is not there, and would be an error to ask for
    static MAX_ID = 2147483647;

    constructor(app) {
        this.app = app;
        this.setupEndpoints();
    }

    setupEndpoints() {
        //The page is the one the router was asked for, which it passes along as X-Original-URI
        this.app.expressApp.get('/page-exists', async (req, res) => {
            const match = /^\/(site|taxon|species)\/(\d+)\/?(\?.*)?$/.exec(req.get('x-original-uri') || '');
            if(!match || Number(match[2]) > PageExists.MAX_ID) {
                res.sendStatus(403);
                return;
            }

            const sql = match[1] == 'site'
                ? 'SELECT 1 FROM tbl_sites WHERE site_id = $1'
                : 'SELECT 1 FROM tbl_taxa_tree_master WHERE taxon_id = $1';

            const pgClient = await this.app.getDbConnection();
            try {
                const result = await pgClient.query(sql, [Number(match[2])]);
                res.sendStatus(result.rows.length > 0 ? 204 : 403);
            }
            catch(error) {
                console.error('PageExists error:', error);
                res.sendStatus(500);
            }
            finally {
                this.app.releaseDbConnection(pgClient);
            }
        });
    }
}

export default PageExists;
