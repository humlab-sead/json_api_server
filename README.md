# SEAD JSON server

This server provides a REST API like `/site/1` which outputs all the information related to that site in an hierarchical JSON format.

Since a MongoDB is used as a backend storage/cache for the JSON documents, the server can also accept simple queries like below.

To find which site has the analysis_entity 131350:
`/search/datasets.analysis_entities.analysis_entity_id/value/131350`

To find all sites which contain the species 'spelta dicoccum':
`/search/lookup_tables.taxa.species/value/spelta%2Fdicoccum`
