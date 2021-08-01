const config = require('../config.js');
const pub_sub = require('./pub-sub.js');
const fas_bq = require('./fas-bq');
const utils = require('./utils');
const aiplatform = require('@google-cloud/aiplatform');
const { instance, prediction } = aiplatform.protos.google.cloud.aiplatform.v1.schema.predict;
const { PredictionServiceClient } = aiplatform.v1;
const clientOptions = {
  apiEndpoint: 'us-central1-aiplatform.googleapis.com',
};
const predictionServiceClient = new PredictionServiceClient(clientOptions);

async function predictTextClassification(dataSetName, tweets, endpointId) {
  let project = config.gcp_projectId;
  let location = config.gcp_project_location;
  let cxmRows = [];

  // Configure the resources
  const endpoint = `projects/${project}/locations/${location}/endpoints/${endpointId}`;

  for (let tweet of tweets) {

    const predictionInstance =
      new instance.TextClassificationPredictionInstance({
        content: tweet.text,
      });
    const instanceValue = predictionInstance.toValue();

    const instances = [instanceValue];
    const request = {
      endpoint,
      instances,
    };

    const [response] = await predictionServiceClient.predict(request);
    console.log('Processing ML Model CXM ',tweet.id);
    for (const predictionResultValue of response.predictions) {
      const predictionResult =
        prediction.ClassificationPredictionResult.fromValue(
          predictionResultValue
        );

      let resultArr = []
      for (const [i, label] of predictionResult.displayNames.entries()) {
        resultArr.push({ label: label, confidences: predictionResult.confidences[i], ids: predictionResult.ids[i] })

        // console.log(`\tDisplay name: ${label}`);
        // console.log(`\tConfidences: ${predictionResult.confidences[i]}`);
        // console.log(`\tIDs: ${predictionResult.ids[i]}\n\n`);
      }
      let cxmRow = {
        id_str: tweet.id,
        model_name: 'CXM',
        model_id: response.deployedModelId,
        result: resultArr
      }
      cxmRows.push(cxmRow);
    }
    if( cxmRows.length > 9 ) {
      fas_bq.insertRowsAsStream(dataSetName, config.bq.table.ml_model, cxmRows);
      cxmRows = []
  }
    utils.sleep(1000);
  }
}

module.exports = { predictTextClassification };