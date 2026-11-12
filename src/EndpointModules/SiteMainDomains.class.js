/*
* Class: SiteMainDomains
* Tells, for every site with analysed samples, which domain its evidence is mainly from - the kind of evidence
* the result map colours the site by.
*
* The domains are those of the facet schema (facet group 999), each defined by a clause on tbl_datasets. A
* site's main domain is the one with the most analysis entities at the site, and if two domains have as many,
* the one listed first in the facet schema. Sites without analysed samples have no main domain and are left out.
*
* The answer only changes when the database does, so it is kept in the graph cache, which is flushed on deploy.
*/
class SiteMainDomains {
    static CACHE_ID = "site_main_domains";

    constructor(app) {
        this.app = app;
        this.setupEndpoints();
    }

    setupEndpoints() {
        /*
        * Endpoint: /sites/main_domains
        * { domains: [{ facet_code, display_title, site_count }], sites: { <site_id>: <facet_code> } }
        */
        this.app.expressApp.get('/sites/main_domains', async (req, res) => {
            try {
                const mainDomains = await this.fetchSiteMainDomains();
                res.header("Content-type", "application/json");
                res.send(JSON.stringify({ domains: mainDomains.domains, sites: mainDomains.sites }));
            }
            catch(error) {
                console.error("SiteMainDomains error:", error);
                res.status(500).send(JSON.stringify({ error: "Internal server error" }));
            }
        });
    }

    async fetchSiteMainDomains() {
        const identifierObject = { cache_id: SiteMainDomains.CACHE_ID };
        if(this.app.useGraphCaching) {
            const cachedData = await this.app.getObjectFromCache("graph_cache", identifierObject);
            if(cachedData !== false) {
                return cachedData;
            }
        }

        let pgClient = await this.app.getDbConnection();
        try {
            const domainsResult = await pgClient.query(`
                select facet.facet_id, facet.facet_code, facet.display_title, facet_clause.clause
                from facet.facet
                join facet.facet_clause on facet_clause.facet_id = facet.facet_id
                where facet.facet_group_id = 999
                order by facet.facet_id
            `);
            const domains = domainsResult.rows;

            //The clauses are SQL from the facet schema - the same the query API filters the domains by - and
            //name tbl_datasets unaliased, so each domain counts its analysis entities with a query of its own
            const perDomainQueries = domains.map((domain, index) => `
                select $${index + 1}::text as facet_code, ${index} as domain_order, tbl_sample_groups.site_id, count(*) as analysis_entities
                from tbl_datasets
                join tbl_analysis_entities using (dataset_id)
                join tbl_physical_samples using (physical_sample_id)
                join tbl_sample_groups using (sample_group_id)
                where ${domain.clause}
                group by tbl_sample_groups.site_id
            `);

            const sitesResult = await pgClient.query(`
                select distinct on (site_id) site_id, facet_code
                from (${perDomainQueries.join(" union all ")}) as per_domain
                order by site_id, analysis_entities desc, domain_order
            `, domains.map(domain => domain.facet_code));

            const sites = {};
            const siteCounts = {};
            sitesResult.rows.forEach(row => {
                sites[row.site_id] = row.facet_code;
                siteCounts[row.facet_code] = (siteCounts[row.facet_code] || 0) + 1;
            });

            const resultObject = {
                cache_id: SiteMainDomains.CACHE_ID,
                domains: domains.map(domain => ({
                    facet_code: domain.facet_code,
                    display_title: domain.display_title,
                    site_count: siteCounts[domain.facet_code] || 0
                })),
                sites: sites
            };

            if(this.app.useGraphCaching) {
                await this.app.saveObjectToCache("graph_cache", identifierObject, resultObject);
            }
            return resultObject;
        }
        finally {
            this.app.releaseDbConnection(pgClient);
        }
    }
}

export default SiteMainDomains;
