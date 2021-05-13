const { BigQuery } = require("@google-cloud/bigquery");

const projectId = "twttr-des-sa-demo-dev";
const datasetId = "Annotations";
const table = "fas_results";


async function insertRowsAsStream(tableId, rows) {
  const bigqueryClient = new BigQuery();

  // Insert data into a table
  try {
    const result = await new Promise((resolve, reject) => {
      bigqueryClient
        .dataset(datasetId)
        .table(tableId)
        .insert(rows)
        .then((results) => {
          console.log(`Inserted ${rows.length} rows`);
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

async function insertResults(results, category) {
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
      }

      var cDate = new Date(tweet.created_at);
      resultRows.push({
        id: tweet.id,
        id_str: tweet.id_str,
        text: tweet.text,
        category: category,
        reply_settings: tweet.reply_settings,
        source: tweet.source,
        author_id: tweet.author_id,
        conversation_id: tweet.conversation_id,
        created_at: BigQuery.datetime(cDate.toISOString()),
        lang: tweet.lang,
        in_reply_to_user_id: tweet.in_reply_to_user_id,
        in_reply_to_screen_name: tweet.in_reply_to_screen_name,
        possibly_sensitive: tweet.possibly_sensitive,
        geo: geoVar,
        favorited: tweet.favorited,
        retweeted: tweet.retweeted,
        quote_count: tweet.quote_count,
        reply_count: tweet.reply_count,
        retweet_count: tweet.retweet_count,
        favorite_count: tweet.favorite_count,
        entities: entitiesVar,
      //  withheld: tweet.withheld,
        user: tweet.user,
        tweet_url: 'http://twitter.com/twitter/status/'+tweet.id_str
      });
    }
  });

  insertRowsAsStream(table, resultRows);
}

module.exports = { insertResults };
