
async function persistFollowerTweets(followers, twitter_handle) {
    followers.forEach(function (follower, index) {
      console.log('persist followers ', index);
      let params = {};
      params.handle = twitter_handle;
      params.followerHandle = follower.username;
      params.discriminator = config.follower_discriminator;
      if (index < config.max_followers)
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
  