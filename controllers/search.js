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
    followers(req.body.handle);
  });
});

async function searchTweetsFollowers(params) {
  var query = { "query": "from:" + params.followerHandle, "maxResults": 500, fromDate: "201905010000", toDate: "202105200000" }
  return new Promise(function (resolve, reject) {
    let axiosConfig = {
      method: 'post',
      url: config.fas_search_followers_url,
      auth: {
        username: config.gnip_username,
        password: config.gnip_password
      },
      data: query
    };
    console.log('follower search query ', JSON.stringify(query));
    axios(axiosConfig)
      .then(function (resp) {
        if (resp != null) {
          console.log('followers search results ',resp.data.results.length);
          fas_svcs.insertResults(resp.data.results, params);
          resolve({ "message": "Query result persisted" });
        }
      })
      .catch(function (error) {
        console.log('ERROR --- ', error);
        resolve(error);
      });
  });
}

async function fullArchiveSearch(reqBody, nextToken) {
  var handle = reqBody.handle;
  if (handle == undefined || handle == null || handle == '')
    return ('Empty Twitter handle');
  var query = { "query": "from:" + handle, "maxResults": 500, fromDate: "202101010000", toDate: "202105200000" }
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
          //console.log('response ',resp.data);
          fas_svcs.insertResults(resp.data.results, reqBody);
          if (resp.data != undefined && resp.data.next != undefined) {
            fullArchiveSearch(reqBody, resp.data.next);
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

async function persistFollowerTweets(followers, twitter_handle) {
  followers.forEach( function ( follower, index)  {
    console.log('persist followers ',index);
    let params = {};
    params.handle = twitter_handle;
    params.followerHandle = follower.username;
    params.discriminator = config.follower_discriminator;
    if( index < config.max_followers )
      searchTweetsFollowers(params);
  });
}

async function followers(twitter_handle) {
  console.log('in followers method');
  let resp = await getUserIdByHandle(twitter_handle);
  return new Promise(function (resolve, reject) {
    console.log("1....");
    let followsConfig = {
      method: 'get',
      url: 'https://api.twitter.com/2/users/' + resp.id + '/followers',
      headers: { 'Authorization': config.twitter_bearer_token }
    };
    axios(followsConfig)
      .then(function (response) {
        if (response.data.data != null) {
          let followers = response.data.data;
          //console.log('followers -- ',followers);
          if (followers != null && followers.length) {
            console.log('insert followers');
            fas_svcs.insertFollowers(followers, twitter_handle);
            persistFollowerTweets(followers, twitter_handle)
          }
          console.log("2....");

        resolve(followers);
        }
      })
      .catch(function (error) {
        reject(error);
      });
  });
}

async function getUserIdByHandle(twitter_handle) {
  return new Promise(function (resolve, reject) {
    let userConfig = {
      method: 'get',
      url: 'https://api.twitter.com/2/users/by/username/' + twitter_handle,
      headers: { 'Authorization': config.twitter_bearer_token }
    };
    axios(userConfig)
      .then(function (response) {
        if (response.data.data != null) {
          resolve(response.data.data);
        }
      })
      .catch(function (error) {
        reject(error);
      });
  });
}

module.exports = router;
