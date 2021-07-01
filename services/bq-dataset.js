const { BigQuery } = require("@google-cloud/bigquery");
const config = require('../config.js');
const bigquery = new BigQuery();
const fs = require('fs');

async function provisionDB(dataSetObj) {
    if (dataSetObj === null)
        return;
    return new Promise(function (resolve, reject) {
        if (dataSetObj.newDataSet === false) {
            resolve('Successfully provisioned DB -- already')
            return;
        }
        createDataSet(dataSetObj.dataSetName).then((dataSetResponse) => {
            console.log('dataSetResponse ', dataSetResponse);
            createTables(dataSetObj.dataSetName).then((tablesResponse) => {
                console.log('tablesResponse ', tablesResponse);
                resolve('Successfully provisioned DB');
            }).catch(function (error) {
                console.log('Error provisioning tables ', error);
                reject({ "error": "Error Provisioning tables " });
            });
        }).catch(function (error) {
            console.log('Error provisioning DB ', error);
            reject({ "error": "Error Provisioning DB " });
        })
    })

}

async function createDataSet(dataSetName) {

    const options = {
        location: 'US',
    };

    console.log('dataSetName -- ', dataSetName);
    //    Create a new dataset
    const [dataset] = await bigquery.createDataset(dataSetName, options);
    const dataSetId = dataset.id;
    console.log(`Dataset ${dataSetId} created.`);

}

async function createTables(datasetId) {
    //create tables
    const fas_schema = fs.readFileSync('./schema/fas_results.json');
    const nlp_schema = fs.readFileSync('./schema/nlp.json');
    const watson_nlp_schema = fs.readFileSync('./schema/watson_nlp.json');
    const [fas_table] = await bigquery.dataset(datasetId).createTable(config.bq.table.fas_results, { schema: JSON.parse(fas_schema), location: 'US' });
    console.log(`Table ${fas_table.id} created.`);
    const [nlp_table] = await bigquery.dataset(datasetId).createTable(config.bq.table.nlp, { schema: JSON.parse(nlp_schema), location: 'US' });
    console.log(`Table ${nlp_table.id} created.`);
    const [watson_nlp_table] = await bigquery.dataset(datasetId).createTable(config.bq.table.watson_nlp, { schema: JSON.parse(watson_nlp_schema), location: 'US' });
    console.log(`Table ${watson_nlp_table.id} created.`);
}

module.exports = { createDataSet, createTables, provisionDB };