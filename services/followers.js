const config = require('../config.js');
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const utils = require('./utils');
const fas_svcs = require('./fas-bq.js');

async function followersGraph(userId, reqBody) {
  //let resp = await getUserIdByHandle(twitter_handle);
  reqBody.followers.parentUserId = userId;
  return new Promise(function (resolve, reject) {
    let followsConfig = {
      method: 'get',
      url: 'https://api.twitter.com/2/users/' + userId + '/followers',
      headers: { 'Authorization': config.twitter_bearer_token }
    };
    let followers;
    axios(followsConfig)
      .then(function (response) {
        if (response.data.data != null) {
          followers = response.data.data;
          if (followers != null && followers.length) {
            fas_svcs.insertFollowers(followers, reqBody);
          }
        }
        resolve(followers);
      })
      .catch(function (error) {
        console.log('followersGraph error -- ',error)
        resolve(error);
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

async function getUserProfiles(userId, reqBody)  {
  console.log('Activating user profiles');
  var query = { "query": "from:"+userId, "maxResults": 10, fromDate: '200801010000', toDate: '202107150000' }
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
    console.log('userProfiles query ', JSON.stringify(query));
    axios(axiosConfig)
      .then(function (resp) {
        if (resp != null) {
          console.log('Search results into BQ and Publish into Topics');
          if (resp.data != null && resp.data.results != null && resp.data.results.length > 0) {
            fas_svcs.insertUserProfiles(resp.data.results, reqBody);
          }
          resolve({ "message": "UserProfiles persisted" });
        }
      })
      .catch(function (error) {
        console.log('ERROR --- ', error);
        resolve(error);
      });
  });

}

module.exports = { followersGraph, getUserProfiles };
