const { BigQuery } = require("@google-cloud/bigquery");
const config = require('../config.js');

async function insertRowsAsStream(datasetId, tableId, rows) {
  const bigqueryClient = new BigQuery();
  // Insert data into a table
  try {
    const result = await new Promise((resolve, reject) => {
      bigqueryClient
        .dataset(datasetId)
        .table(tableId)
        .insert(rows)
        .then((results) => {
          console.log(`Inserted ${rows.length} rows into ${tableId} for dataset ${datasetId}`);
          resolve(rows);
        })
        .catch((err) => {
          reject(err);
        });
    });
  } catch (error) {
    console.log("----BQ JSON Error --- \n ", JSON.stringify(error), "\n");
    throw new Error(error);
  }
}

async function insertFollowers(followers, reqBody) {
  var followersRow = [];
  followers.forEach( function ( follower, index)  {
    followersRow.push({
      category: reqBody.fullArchiveSearch.category,
      subcategory: reqBody.fullArchiveSearch.subCategory,
      parent_user_id: reqBody.followers.parentUserId,
      user_id: follower.id,
      name: follower.name,
      username: follower.username
    });
  });
  insertRowsAsStream(reqBody.dataSet.dataSetName , config.followers_table, followersRow);
}

async function insertResults(results, reqBody) {
  var resultRows = [];
  results.forEach(function (tweet, index) {
    //console.log('FAS Response -- ', tweet);
    if (tweet) {
      if (tweet.geo != undefined) {
        var geoVar = tweet.geo;
        if (tweet.geo.coordinates != undefined) {
          geoVar.coordinates = tweet.geo.coordinates;
          if (tweet.geo.coordinates.coordinates != undefined && Array.isArray(tweet.geo.coordinates.coordinates) && tweet.geo.coordinates.coordinates.length) {
            geoVar.coordinates.coordinates = tweet.geo.coordinates.coordinates;
          }
        }
      }

      if (tweet.entities != undefined) {
        var entitiesVar = tweet.entities;
        if (tweet.entities.urls === undefined)
          entitiesVar.urls = [];
        else
        entitiesVar.urls = [];
        // TODO: ^^ correct it
        if (tweet.entities.user_mentions === undefined)
          entitiesVar.user_mentions = [];
        if (tweet.entities.hashtags === undefined)
          entitiesVar.hashtags = [];
        //if (tweet.entities.media === undefined)
          entitiesVar.media = [];
          entitiesVar.symbols = [];
      }

      var cDate = new Date(tweet.created_at);
      let row = {
        id: tweet.id,
        id_str: tweet.id_str,
        text: tweet.text,
        category: reqBody.fullArchiveSearch.category,
        subcategory: reqBody.fullArchiveSearch.subCategory,
        reply_settings: tweet.reply_settings,
        source: tweet.source,
        author_id: tweet.author_id,
        conversation_id: tweet.conversation_id,
        created_at: BigQuery.datetime(cDate.toISOString()),
        lang: tweet.lang,
        in_reply_to_user_id: tweet.in_reply_to_user_id,
        in_reply_to_screen_name: tweet.in_reply_to_screen_name,
        possibly_sensitive: tweet.possibly_sensitive,
        //geo: geoVar,
        favorited: tweet.favorited,
        retweeted: tweet.retweeted,
        quote_count: tweet.quote_count,
        reply_count: tweet.reply_count,
        retweet_count: tweet.retweet_count,
        favorite_count: tweet.favorite_count,
        entities: entitiesVar,
        user: tweet.user,
        tweet_url: 'http://twitter.com/twitter/status/'+tweet.id_str
      };
      if( reqBody.discriminator != null && reqBody.discriminator === config.follower_discriminator)  {
        row.follower_handle = reqBody.followerHandle;
      }
      resultRows.push(row);
    }
  });

  let resultsTable = config.results_table;
  if( reqBody.discriminator != null && reqBody.discriminator === config.follower_discriminator)
    resultsTable = config.followers_tweet_table;
  insertRowsAsStream(reqBody.dataSet.dataSetName, resultsTable, resultRows);
}

async function insertCountsResults(results, bucket, reqBody) {
  var resultRows = [];
  results.forEach(function (result, index) {
    //console.log('FAS Response -- ', result);
    if (result) {
      var dateStr = result.timePeriod;
      var tpDate = new Date(dateStr.substr(0,4),dateStr.substr(4,2),dateStr.substr(6,2),dateStr.substr(8,2),dateStr.substr(10,2));
      //console.log('tpdate -- ',tpDate.toISOString());
      resultRows.push({
        timeperiod: BigQuery.datetime(tpDate.toISOString()),
        results: result.count,
        bucket: bucket,
        category: reqBody.fullArchiveSearch.category,
        subcategory: reqBody.fullArchiveSearch.subCategory
      });
    }
  });

  insertRowsAsStream(reqBody.dataSet.dataSetName, config.bq.table.fas_search_counts, resultRows);
}

async function queryBQTable(sqlQuery)  {
  const bigqueryClient = new BigQuery();

    console.log('queryBQTable SQL ',sqlQuery);

    const options = {
      query: sqlQuery,
      location: 'US',
    };

    const [rows] = await bigqueryClient.query(options);

    console.log('Query Results:');
    return rows;
}

async function insertUserProfiles(results, reqBody) {
  let tweet = results[0]; // first row as user row
  var resultRows = [];
    //console.log('FAS Response -- ', tweet);
    if (tweet) {
      var cDate = new Date(tweet.created_at);
      let row = {
        category: reqBody.fullArchiveSearch.category,
        subcategory: reqBody.fullArchiveSearch.subCategory,
        id: tweet.user.id,
        id_str: tweet.user.id_str,
        name: tweet.user.name,
        screen_name: tweet.user.screen_name,
        location: tweet.user.location,
        url: tweet.user.url,
        description: tweet.user.description,
        protected: tweet.user.protected,
        verified: tweet.user.verified,
        followers_count: tweet.user.followers_count,
        friends_count: tweet.user.friends_count,
        listed_count: tweet.user.listed_count,
        favourites_count: tweet.user.favourites_count,
        statuses_count: tweet.user.statuses_count,
        created_at: tweet.user.created_at,
        lang: tweet.user.lang,
        following: tweet.user.following,
        derived: tweet.user.derived,
        time_zone: tweet.user.time_zone,
        translator_type: tweet.user.translator_type,
      };
      resultRows.push(row);
    }

  let resultsTable = config.bq.table.users;
  insertRowsAsStream(reqBody.dataSet.dataSetName, resultsTable, resultRows);
}


module.exports = { insertResults, insertFollowers, insertRowsAsStream, insertCountsResults, queryBQTable, insertUserProfiles };
