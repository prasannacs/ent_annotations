const express = require("express");
const fs = require('fs');
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const config = require('../config.js');
const fas_svcs = require('.././services/fas-bq.js');

var ruleCategory;
axiosRetry(axios, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  shouldResetTimeout: true,
  retryCondition: (axiosError) => {
    return true;
  },
});

const router = express.Router();

router.get("/", function (req, res) {
  res.send("Twitter Enterprise API Search Application");
});


router.post("/fas", function (req, res) {
  fullArchiveSearch(req.body).then(function (response) {
      res.status(200).send(response);
  })
});

async function fullArchiveSearch(reqBody, nextToken) {
  var handle = reqBody.handle;
  if( handle == undefined || handle == null || handle == '') 
    return('Empty Twitter handle');
  var query = { "query": "from:"+handle, "maxResults": 500,fromDate: "202101010000", toDate: "202105100000"}
  if( nextToken != undefined && nextToken != null )  
    query.next = nextToken;
  return new Promise(function (resolve, reject) {
    let axiosConfig = {
      method: 'post',
      url: config.fas_search_url,
      auth: {
        username: config.gnip_username,
        password: config.gnip_password
      },
      data: query
    };
    console.log('query ',JSON.stringify(query));
    axios(axiosConfig)
      .then(function (resp) {
        if (resp != null) {
          //console.log('response ',response.data);
          fas_svcs.insertResults(resp.data.results, handle);
          if( resp.data != undefined && resp.data.next != undefined)  {
            fullArchiveSearch(reqBody, resp.data.next);
          }
          resolve({"message":"Query result persisted"});
        }
      })
      .catch(function (error) {
        console.log('ERROR --- ', error);
        resolve(error);
      });
  });
}

module.exports = router;
