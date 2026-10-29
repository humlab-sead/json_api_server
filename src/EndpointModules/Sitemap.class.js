/*
* Class: Sitemap
* Sitemaps of the client's site reports and taxon datasheets, for search engines.
*
* The client is a single page application, so crawlers only find these pages by following
* links in result lists that are themselves rendered by javascript. Listing them here is
* what gets them found at all. The client's own sitemap (the front page and the domains) is
* static; these two are linked next to it from its robots.txt.
*
* The router serves them at the root, as /sitemap-sites.xml and /sitemap-taxa.xml: a sitemap
* may only list URLs under the path it is served from, and the pages are at /site/ and /taxon/.
*
* Only pages with data in them are listed - sites with at least one analysed sample and taxa
* with at least one abundance. The others render as near-empty pages, which do the site's
* standing with search engines more harm than good.
*/
class Sitemap {
    constructor(app) {
        this.app = app;
        this.setupEndpoints();
    }

    setupEndpoints() {
        this.app.expressApp.get('/sitemap/sites.xml', async (req, res) => {
            //The site's own date_updated is rarely touched when data is added, its datasets' are
            const sql = `
                SELECT sg.site_id AS id, GREATEST(MAX(s.date_updated), MAX(d.date_updated)) AS lastmod
                FROM tbl_sites s
                JOIN tbl_sample_groups sg ON sg.site_id = s.site_id
                JOIN tbl_physical_samples ps ON ps.sample_group_id = sg.sample_group_id
                JOIN tbl_analysis_entities ae ON ae.physical_sample_id = ps.physical_sample_id
                JOIN tbl_datasets d ON d.dataset_id = ae.dataset_id
                GROUP BY sg.site_id
                ORDER BY sg.site_id`;
            await this.sendSitemap(req, res, sql, '/site/');
        });

        this.app.expressApp.get('/sitemap/taxa.xml', async (req, res) => {
            const sql = `
                SELECT t.taxon_id AS id, t.date_updated AS lastmod
                FROM tbl_taxa_tree_master t
                WHERE EXISTS (SELECT 1 FROM tbl_abundances a WHERE a.taxon_id = t.taxon_id)
                ORDER BY t.taxon_id`;
            await this.sendSitemap(req, res, sql, '/taxon/');
        });
    }

    async sendSitemap(req, res, sql, pathPrefix) {
        const pgClient = await this.app.getDbConnection();
        try {
            const result = await pgClient.query(sql);
            res.header('Content-type', 'application/xml');
            res.send(this.buildUrlset(result.rows, this.getOrigin(req), pathPrefix));
        }
        catch(error) {
            console.error('Sitemap error:', error);
            res.status(500).send('Could not build the sitemap');
        }
        finally {
            this.app.releaseDbConnection(pgClient);
        }
    }

    /*
    * Function: getOrigin
    * Where the pages are. Not authHandler.publicOrigin: DOMAIN is not handed to this container,
    * which leaves that at https://localhost unless PUBLIC_ORIGIN is set. The router passes the
    * host the request came in on, which is the client's.
    */
    getOrigin(req) {
        if(process.env.PUBLIC_ORIGIN) {
            return process.env.PUBLIC_ORIGIN.replace(/\/+$/, '');
        }
        return 'https://'+req.get('host');
    }

    /*
    * Function: buildUrlset
    * A sitemap of the rows, each an id and an optional lastmod date, as pages under pathPrefix.
    */
    buildUrlset(rows, origin, pathPrefix) {
        const urls = rows.map(row => {
            let url = '  <url>\n    <loc>'+origin+pathPrefix+row.id+'</loc>\n';
            if(row.lastmod) {
                url += '    <lastmod>'+new Date(row.lastmod).toISOString().substring(0, 10)+'</lastmod>\n';
            }
            return url+'  </url>';
        });
        return '<?xml version="1.0" encoding="UTF-8"?>\n'
            +'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            +urls.join('\n')+'\n'
            +'</urlset>\n';
    }
}

export default Sitemap;
