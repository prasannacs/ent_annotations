const express = require("express");
const fs = require('fs');
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const config = require('../config.js');
const fas_svcs = require('.././services/fas-bq.js');
const pub_sub = require('.././services/pub-sub.js');
const bq_dataset = require('.././services/bq-dataset.js');
const utils = require('.././services/utils.js');

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

router.get("/followers", function (req, res) {
  fas_svcs.queryBQTable(utils.getEngagementsSQL(req.body)).then((rows) => {
    getFollowers(rows, req.body);
  });
  res.send('followers');
});

router.get("/followers/userprofiles", function (req, res) {
  fas_svcs.queryBQTable(utils.getFollowsSQL(req.body)).then((rows) => {
    getUserProfiles(rows, req.body);
  });
  res.send('user profiles');
});

async function getUserProfiles(tweets, reqBody) {
  let topicName = config.user_profiles_topic + '_' + reqBody.dataSet.dataSetName + '_' + reqBody.fullArchiveSearch.category;

  pub_sub.createTopic(topicName).then(() => {
    let subscriptionName = topicName + 'sub';
    reqBody.followers.topicName = topicName;
    pub_sub.createSubscription(topicName, subscriptionName).then(() => {
      console.log('User Profiles Subscription created ', subscriptionName);
      reqBody.followers.subscriptionName = subscriptionName;
      pub_sub.subscribeWithFlowControlSettings(reqBody, 4, 'userProfiles');
      pub_sub.publishUserProfiles(tweets, reqBody.fullArchiveSearch.category, topicName);
    });
  }).catch(function (error) {
    console.log('getFollowers topic creation error ', error);
  })
}

async function getFollowers(tweets, reqBody) {
  let topicName = config.followers_topic + '_' + reqBody.dataSet.dataSetName + '_' + reqBody.fullArchiveSearch.category;

  pub_sub.createTopic(topicName).then(() => {
    let subscriptionName = topicName + 'sub';
    reqBody.followers.topicName = topicName;
    pub_sub.createSubscription(topicName, subscriptionName).then(() => {
      console.log('Followers Subscription created ', subscriptionName);
      reqBody.followers.subscriptionName = subscriptionName;
      pub_sub.subscribeWithFlowControlSettings(reqBody, 1, 'follows');
      pub_sub.publishTweets(tweets, reqBody.fullArchiveSearch.category, topicName);
    });
  }).catch(function (error) {
    console.log('getFollowers topic creation error ', error);
  })
}

router.post("/fas/provisionDB", function (req, res) {
  if (req.body.dataSet === null)
    res.send("Invalid input params - Pass a valid JSON dataSet object");
  if (req.body.dataSet.newDataSet === true) {
    return new Promise(function (resolve, reject) {
      bq_dataset.createDataSet(req.body.dataSet.dataSetName).then((dataSetResponse) => {
        console.log('dataSetResponse ', dataSetResponse);
        bq_dataset.createTables(req.body.dataSet.dataSetName).then((tablesResponse) => {
          console.log('tablesResponse ', tablesResponse);
          res.send(201, { 'status': 'Successfully provisioned DB' });
        }).catch(function (error) {
          console.log('Error provisioning tables ', error);
          res.send(503, { "error": "Error Provisioning tables " });
        });
      }).catch(function (error) {
        console.log('Error provisioning DB ', error);
        res.send(503, { "error": "Error Provisioning DB " });
      })
    })

  }
});

router.post("/fas", function (req, res) {
  bq_dataset.provisionDB(req.body.dataSet).then(function (status) {
    console.log('DB provisioning status ', status);
    if (status != null && status.includes('Successfully provisioned')) {
      pub_sub.setupMsgInfra(req.body).then(function (value) {
        console.log('Value ', value);
        if (value != null) {
          req.body.topicName = value;
          fullArchiveSearch(req.body).then(function (response) {
            res.status(200).send(response);
            fasSearchCounts(req.body, null);
          });
        }
      })
    }
  })

});

async function fullArchiveSearch(reqBody, nextToken) {
  // validate requestBody before Search
  var nlpSwitch = reqBody.naturalLanguage.on;
  let mlSwitch = reqBody.machineLearning.on;
  var fas = reqBody.fullArchiveSearch;
  var query = { "query": fas.query, "maxResults": 500, fromDate: fas.fromDate, toDate: fas.toDate }
  if (nextToken != undefined && nextToken != null)
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
    console.log('query ', JSON.stringify(query));
    axios(axiosConfig)
      .then(function (resp) {
        if (resp != null) {
          console.log('Search results into BQ and Publish into Topics');
          if (resp.data != null && resp.data.results != null && resp.data.results.length > 0) {
            fas_svcs.insertResults(resp.data.results, reqBody);
            // publish to topic
            if (nlpSwitch === true || mlSwitch === true)  {
              pub_sub.publishTweets(resp.data.results, fas.category, reqBody.topicName);
              console.log('Tweets published to topic ',reqBody.topicName);
            }
              
          }
          if (resp.data != undefined && resp.data.next != undefined) {
            fullArchiveSearch(reqBody, resp.data.next);
          } else {
            // no next token - end of FAS insert followers
            if (reqBody.followers.followers_graph === true) {
              
              fas_svcs.queryBQTable(utils.getEngagementsSQL(reqBody)).then((rows) => {
                getFollowers(rows, reqBody);
              });
            }
          }
          resolve({ "message": "Query result persisted" });
        }
      })
      .catch(function (error) {
        console.log('ERROR --- ', error);
        resolve(error);
      });
  });
}

async function fasSearchCounts(reqBody, nextToken) {
  let fas = reqBody.fullArchiveSearch;
  var query = { "query": fas.query, "fromDate": fas.fromDate, "toDate": fas.toDate, "bucket": "day" };
  if (nextToken != undefined && nextToken != null) {
    query.next = nextToken;
  }
  console.log('fasSearchCounts query ', JSON.stringify(query));
  return new Promise(function (resolve, reject) {
    let axiosConfig = {
      method: 'post',
      url: config.fas_search_counts_url,
      auth: {
        username: config.gnip_username,
        password: config.gnip_password
      },
      data: query
    };
    axios(axiosConfig)
      .then(function (response) {
        if (response != null) {
          if (response.data.results != null) {
            fas_svcs.insertCountsResults(response.data.results, 'day', reqBody);
            if (response.data.next != undefined && response.data.next != null) {
              fasSearchCounts(reqBody, response.data.next);
            } else {
              // do nothing and thread will gracefully die
              console.log('search counts all done');
            }
          }
          resolve(response.data);
        }
      })
      .catch(function (error) {
        console.log('ERROR --- ', error);
        reject(error);
      });
  });
}

module.exports = router;
