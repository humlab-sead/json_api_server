/*
* Class: SiteSummary
* A short summary of a site for the result map's popup - enough to tell whether the site is worth opening,
* without fetching the whole site, which can be megabytes.
*
* Most of it is read from the built site in the sites collection, so that, e.g., its dating agrees with the
* site report's. A site not built yet is built first, as opening its site report would. The evidence comes
* from SiteMainDomains, ordered as the main domain the site is colored by on the map is chosen.
*/
class SiteSummary {
    //The most specific kind of place first; a site is placed by the first of these it has, and its country
    static PLACE_TYPES = [
        "Settlement",
        "Archaeological site",
        "Street address",
        "Unprocessed Bugs Transfer",
        "Island",
        "Water body",
        "Historical administrative unit",
        "Sub-country administrative region",
        "Aggregate/non-admin geographical region",
        "Geographical area"
    ];

    static SITE_PROJECTION = {
        _id: 0,
        site_id: 1,
        site_name: 1,
        national_site_identifier: 1,
        "location.location_name": 1,
        "location.location_type": 1,
        "biblio.biblio_id": 1,
        "sample_groups.biblio.biblio_id": 1,
        "sample_groups.physical_samples.physical_sample_id": 1,
        "datasets.biblio_id": 1,
        "lookup_tables.biblio": 1,
        "age_summary.bp_year": 1,
        "age_summary.older": 1,
        "age_summary.younger": 1,
        "age_summary.datings.method_name": 1,
        "age_summary.datings.older": 1,
        "age_summary.datings.younger": 1,
        "age_summary.datings.open_older": 1,
        "age_summary.datings.open_younger": 1,
        "age_summary.datings.radiocarbon_years_count": 1,
        "age_summary.datings.sample_count": 1
    };

    constructor(app) {
        this.app = app;
        this.setupEndpoints();
    }

    setupEndpoints() {
        /*
        * Endpoint: /summary/site/:siteId
        */
        this.app.expressApp.get('/summary/site/:siteId', async (req, res) => {
            const siteId = Number(req.params.siteId);
            if(!Number.isInteger(siteId) || siteId <= 0) {
                res.status(400).send(JSON.stringify({ error: "Bad input - should be a site ID" }));
                return;
            }

            try {
                const summary = await this.fetchSiteSummary(siteId);
                if(!summary) {
                    res.status(404).send(JSON.stringify({ error: "Site not found" }));
                    return;
                }
                res.header("Content-type", "application/json");
                res.send(JSON.stringify(summary));
            }
            catch(error) {
                console.error("SiteSummary error:", error);
                res.status(500).send(JSON.stringify({ error: "Internal server error" }));
            }
        });
    }

    async fetchSiteSummary(siteId) {
        const [site, evidence] = await Promise.all([
            this.fetchSite(siteId),
            this.app.siteMainDomains.fetchSiteDomainEvidence(siteId)
        ]);
        if(!site) {
            return null;
        }

        const sampleGroups = site.sample_groups || [];
        const references = this.getReferences(site);

        return {
            site_id: site.site_id,
            site_name: site.site_name,
            national_site_identifier: site.national_site_identifier,
            place: this.getPlace(site.location || []),
            evidence: evidence,
            sample_group_count: sampleGroups.length,
            sample_count: sampleGroups.reduce((count, sampleGroup) => count + (sampleGroup.physical_samples || []).length, 0),
            age_summary: site.age_summary && site.age_summary.datings && site.age_summary.datings.length > 0 ? site.age_summary : null,
            reference_count: references.length,
            first_reference: references.length > 0 ? references[0] : null
        };
    }

    async fetchSite(siteId) {
        const sites = this.app.mongo.collection("sites");
        let site = await sites.findOne({ site_id: siteId }, { projection: SiteSummary.SITE_PROJECTION });
        if(!site) {
            //Not built yet - built and kept in the sites collection, as for the site report
            const builtSite = await this.app.getSitePostgres(siteId);
            if(!builtSite || builtSite.site_id == null) {
                return null;
            }
            site = await sites.findOne({ site_id: siteId }, { projection: SiteSummary.SITE_PROJECTION }) || builtSite;
        }
        return site;
    }

    /*
    * Function: getPlace
    * Where the site is in a few words: its most specific place and its country, e.g. ["Slöinge", "Sweden"].
    */
    getPlace(locations) {
        const named = locations
            .map(location => ({ name: (location.location_name || "").trim(), type: location.location_type }))
            .filter(location => location.name != "");

        const place = [];
        for(const type of SiteSummary.PLACE_TYPES) {
            const location = named.find(location => location.type == type);
            if(location) {
                place.push(location.name);
                break;
            }
        }
        const country = named.find(location => location.type == "Country");
        if(country && !place.includes(country.name)) {
            place.push(country.name);
        }
        return place;
    }

    /*
    * Function: getReferences
    * Every reference the site, its sample groups and its datasets cite, the site's own first - as the site
    * report lists them.
    */
    getReferences(site) {
        const biblioIds = [];
        const add = (biblioId) => {
            if(biblioId != null && !biblioIds.includes(biblioId)) {
                biblioIds.push(biblioId);
            }
        };
        (site.biblio || []).forEach(biblio => add(biblio.biblio_id));
        (site.sample_groups || []).forEach(sampleGroup => (sampleGroup.biblio || []).forEach(biblio => add(biblio.biblio_id)));
        (site.datasets || []).forEach(dataset => add(dataset.biblio_id));

        const biblioLookup = (site.lookup_tables && site.lookup_tables.biblio) || [];
        return biblioIds
            .map(biblioId => biblioLookup.find(biblio => biblio.biblio_id == biblioId))
            .filter(biblio => biblio)
            .map(biblio => ({
                biblio_id: biblio.biblio_id,
                full_reference: biblio.full_reference,
                authors: biblio.authors,
                year: biblio.year,
                title: biblio.title,
                doi: biblio.doi
            }));
    }
}

export default SiteSummary;
