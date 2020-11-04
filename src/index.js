require('dotenv/config');
const express = require('express');
const cors = require('cors')
const { Client, Pool } = require('pg')


const datingMethodGroups = [3, 19, 20];

console.log("Starting up SEAD Data Server");

const pgPool = new Pool({
    user: process.env.POSTGRES_USER,
    host: process.env.POSTGRES_HOST,
    database: process.env.POSTGRES_DATABASE,
    password:process.env.POSTGRES_PASS,
    port: process.env.POSTGRES_PORT,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

const app = express();
app.use(cors());

async function query(sql, params = []) {
    let pgClient = await pgPool.connect();
    let resultData = await pgClient.query(sql, params);
    pgClient.release();
    return resultData;
}

app.get('/*', (req, res, next) => {
    console.log("REQ:", req.url);
    next();
});

app.get('/site/:siteId', async (req, res) => {
    //Returns general metadata about the site - but NOT the samples or analyses
    await query('SELECT * FROM tbl_sites WHERE site_id=$1', [req.params.siteId]).then((data) => {
        return res.send(data);
    });
});

app.get('/site/:siteId/samples', async (req, res) => {
    await query('SELECT * FROM tbl_sites WHERE site_id=$1', [req.params.siteId]).then((data) => {
        return res.send(data);
    });
});


app.get('/site/:siteId/analyses', async (req, res) => {

    let analyses = [];
    let sampleGroupsRes = await getSampleGroupsBySiteId(req.params.siteId);
    
    let sampleFetchPromises = [];
    for(let key in sampleGroupsRes.rows) {
        sampleFetchPromises.push(getPhysicalSamplesBySampleGroupId(sampleGroupsRes.rows[key].sample_group_id));
    }
    
    console.log("Executing", sampleFetchPromises.length, "queries");
    let data = await Promise.all(sampleFetchPromises);
    let physicalSamples = data[0].rows;
    getAnalysisEntitiesByPhysicalSamples(physicalSamples);
    //console.log(data[0].rows)
    
    let output = sampleGroupsRes;
    res.send(output);
});

app.get('/dataset/:datasetId', async (req, res) => {

    let dataset = {
        dataset_id: req.params.datasetId
    };

    let data = await query('SELECT * FROM tbl_datasets WHERE dataset_id=$1', [req.params.datasetId]);
    dataset = data.rows[0];

    let method = await query('SELECT * FROM tbl_methods WHERE method_id=$1', [data.rows[0].method_id]);
    dataset.method = method.rows[0];

    let ae = await query('SELECT * FROM tbl_analysis_entities WHERE dataset_id=$1', [dataset.dataset_id]);
    dataset.analysis_entities = ae.rows;
    

    if(dataset.biblio_id != null) {
        dataset.biblio = await fetchBiblioByDatasetId(dataset.dataset_id);
    }

    //If dataset belongs to certain methods, it might include dating data, so fetch it
    if(datingMethodGroups.includes(dataset.method.method_group_id)) {
        if(dataset.analysis_entities.length > 0) {
            await fetchDatingToPeriodData(dataset.analysis_entities);
        }
    }
    
    await fetchPhysicalSamplesByAnalysisEntities(dataset.analysis_entities);

    res.send(dataset);
});


async function fetchPhysicalSamplesByAnalysisEntities(analysisEntities) {

    if(analysisEntities.length < 1) {
        return false;
    }

    let sampleIdsExploded = "";
    for(let key in analysisEntities) {
        sampleIdsExploded += analysisEntities[key].physical_sample_id+",";
    }
    sampleIdsExploded = sampleIdsExploded.substring(0, sampleIdsExploded.length-1);

    let physicalSamplesResult = await query('SELECT * FROM tbl_physical_samples WHERE physical_sample_id IN ('+sampleIdsExploded+')');

    for(let key in physicalSamplesResult.rows) {
        let sample = physicalSamplesResult.rows[key];
        for(let aeKey in analysisEntities) {
            if(analysisEntities[aeKey].physical_sample_id == sample.physical_sample_id) {
                analysisEntities[aeKey].physical_sample = sample;
            }
        }
    }

    return physicalSamplesResult.rows;
}

async function fetchBiblioByDatasetId(datasetId) {
    let biblio = await query('SELECT * FROM tbl_biblio WHERE dataset_id=$1', [datasetId]);
    return biblio.rows;
}

async function fetchDatingToPeriodData(analysisEntities = []) {
    if(analysisEntities.length < 1) {
        return false;
    }

    let aeIdsExploded = "";
    for(let key in analysisEntities) {
        aeIdsExploded += analysisEntities[key].analysis_entity_id+",";
    }
    aeIdsExploded = aeIdsExploded.substring(0, aeIdsExploded.length-1);

    let sql = 'SELECT * FROM tbl_relative_dates INNER JOIN tbl_relative_ages ON tbl_relative_ages.relative_age_id=tbl_relative_dates.relative_age_id WHERE tbl_relative_dates.analysis_entity_id IN ('+aeIdsExploded+')';

    let ageData = await query(sql);

    //Inserting age data into respective analysis entity - this assumes a 1:1 relationship between the two
    for(let rowKey in ageData.rows) {
        for(let aeKey in analysisEntities) {
            if(analysisEntities[aeKey].analysis_entity_id == ageData.rows[rowKey].analysis_entity_id){
                analysisEntities[aeKey].age_data = ageData.rows[rowKey];
            }
        }
    }

    return ageData.rows;
}


async function getSampleGroupsBySiteId(siteId) {
    return await query('SELECT * FROM tbl_sample_groups WHERE site_id=$1', [siteId]);
}

async function getPhysicalSamplesBySampleGroupId(sampleGroupId) {
    return await query('SELECT * FROM tbl_physical_samples WHERE sample_group_id=$1', [sampleGroupId]);
}

async function getDataBySampleGroupId(sampleGroupId) {
    let sql = "";
    sql += "SELECT * ";
    sql += "FROM tbl_physical_samples ";
    sql += "LEFT JOIN tbl_analysis_entities ON tbl_physical_samples.analysis_entity_id = tbl_analysis_entities.analysis_entity_id "
    sql += "WHERE sample_group_id=$1";
    return await pgClient.query(sql, [sampleGroupId]);
}

async function getAnalysisEntitiesByPhysicalSamples(physicalSamples) {
    let sql = "SELECT * FROM tbl_analysis_entities WHERE tbl_analysis_entities.physical_sample_id=$1";
    let queryPromises = [];

    for(let key in physicalSamples) {
        queryPromises.push(query(sql, [physicalSamples[key].physical_sample_id]));
        //queryPromises.push(pgPool.query(sql, [physicalSamples[key].physical_sample_id]));
    }

    console.log("Executing", queryPromises.length, "queries");

    Promise.all(queryPromises).then((data) => {
        console.log("Done!");
    });
}

process.on('SIGTERM', function (code) {
    console.log('SIGTERM received...', code);
});


const server = app.listen(process.env.API_PORT, () =>
  console.log("Webserver started, listening at port", process.env.API_PORT),
);

process.on('SIGINT', function (code) {
    console.log(code, 'received', );
    pgPool.end();
    server.close(() => {
        console.log('Server shutdown');
        process.exit(0);
    });
});