
require('dotenv/config');
const SeadJsonServer = require('./SeadJsonServer.class')

if(typeof process.env.POSTGRES_HOST == "undefined") {
    throw new Error("Didn't find any .env file! Please copy .env-example to .env and fill it out.");
}


const seadDataServer = new SeadJsonServer();

process.on('SIGTERM', function (code) {
    console.log('SIGTERM received...', code);
});

process.on('SIGINT', function (code) {
    console.log(code, 'received', );
    seadDataServer.shutdown();
});

