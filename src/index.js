require('dotenv/config');
const express = require('express');
const { Client, Pool } = require('pg')

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

async function query(sql, params) {
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

app.get('/site/:siteId/analysis/geological_period', async (req, res) => {
    
});

app.get('/dataset/:datasetId', async (req, res) => {
    pgClient.query('SELECT * FROM tbl_sites WHERE site_id=$1', [req.params.siteId]).then((rows) => {
        return res.send(rows);
    });
});


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