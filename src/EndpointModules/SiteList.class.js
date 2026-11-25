/*
* Class: SiteList
* The "site list" export: one row per site with where it is and how much it holds, without the data itself.
* The client's "Export sites" dialog offers it as the alternative to exporting all data, and writes it out as
* XLSX, CSV or JSON.
*
* A site's domain is its main domain, the kind of evidence the result map colours the site by
* (SiteMainDomains): the domain with the most analysis entities at the site. Sites without analysed samples
* have none.
*/
class SiteList {
    constructor(app) {
        this.app = app;
        this.setupEndpoints();
    }

    setupEndpoints() {
        /*
        * Endpoint: POST /export/site_list
        * Body: [site_id, ...]
        * [{ site_id, site_name, latitude, longitude, altitude, national_site_identifier, domain,
        *    sample_groups_num, samples_num }], ordered by site_id. Unknown ids are left out.
        */
        this.app.expressApp.post('/export/site_list', async (req, res) => {
            const siteIds = req.body;
            if(!this.app.isValidSiteIdsInput(siteIds)) {
                res.status(400).json({ error: "Bad input - should be an array of site IDs" });
                return;
            }
            try {
                res.status(200).json(await this.fetchSiteList(siteIds.map(Number)));
            }
            catch(error) {
                console.error("SiteList error:", error);
                res.status(500).json({ error: "Internal server error" });
            }
        });
    }

    async fetchSiteList(siteIds) {
        const mainDomains = await this.app.siteMainDomains.fetchSiteMainDomains();
        const domainTitles = new Map(mainDomains.domains.map(domain => [domain.facet_code, domain.display_title]));

        const pgClient = await this.app.getDbConnection();
        try {
            const result = await pgClient.query(`
                select s.site_id, s.site_name, s.latitude_dd, s.longitude_dd, s.altitude, s.national_site_identifier,
                    coalesce(counts.sample_groups_num, 0) as sample_groups_num,
                    coalesce(counts.samples_num, 0) as samples_num
                from tbl_sites s
                left join (
                    select sg.site_id,
                        count(distinct sg.sample_group_id) as sample_groups_num,
                        count(ps.physical_sample_id) as samples_num
                    from tbl_sample_groups sg
                    left join tbl_physical_samples ps using (sample_group_id)
                    where sg.site_id = any($1::int4[])
                    group by sg.site_id
                ) counts using (site_id)
                where s.site_id = any($1::int4[])
                order by s.site_id
            `, [siteIds]);

            //numeric and count columns come back from pg as strings
            const number = value => (value === null || value === undefined ? null : Number(value));
            return result.rows.map(row => {
                const facetCode = mainDomains.sites[row.site_id];
                return {
                    site_id: row.site_id,
                    site_name: row.site_name,
                    latitude: number(row.latitude_dd),
                    longitude: number(row.longitude_dd),
                    altitude: number(row.altitude),
                    national_site_identifier: row.national_site_identifier,
                    domain: facetCode ? (domainTitles.get(facetCode) || facetCode) : null,
                    sample_groups_num: number(row.sample_groups_num),
                    samples_num: number(row.samples_num),
                };
            });
        }
        finally {
            this.app.releaseDbConnection(pgClient);
        }
    }
}

export default SiteList;
