const language = require('@google-cloud/language');
const config = require('../config.js');
const pub_sub = require('./pub-sub.js');
const fas_bq = require('./fas-bq');
const utils = require('./utils');

async function pullTweets() {
    console.log('subscription name ', config.nlp_messages_to_pull);
    let tweets = await pub_sub.synchronousPull(config.gcp_projectId, config.nlp_subscription, config.nlp_messages_to_pull);

    console.log('Tweets pulled -- ', tweets.length);
    if (tweets != null && tweets.length > 0) {
        annotateText(tweets);
    }
}

async function annotateText(tweets) {
    const client = new language.LanguageServiceClient();
    const features = {
        //"extractSyntax": true,
        "extractEntities": true,
        "extractDocumentSentiment": true,
        "extractEntitySentiment": true
    }
    var nlpRows = [];
    for (let tweet of tweets) {
        const document = {
            content: tweet.text,
            type: 'PLAIN_TEXT',
        };
        const [result] = await client.annotateText({ document: document, features: features });
        const sentiment = result.documentSentiment;
        const entities = result.entities;

        // console.log(`Text: ${tweet.text}`);
        // console.log(`result: ${result}`);
        // console.log(`Sentiment score: ${sentiment.score}`);
        // console.log(`Sentiment magnitude: ${sentiment.magnitude}`);
        // console.log('Entities:');
        var entityRowArr = [];

        entities.forEach(entity => {
            var entityRow = {};
            entityRow.name = entity.name;
            entityRow.type = entity.type;
            entityRow.salience = entity.salience;
            // console.log(entity.name);
            // console.log(` - Type: ${entity.type}, Salience: ${entity.salience}`);
            if (entity.metadata && entity.metadata.wikipedia_url) {
                // console.log(` - Wikipedia URL: ${entity.metadata.wikipedia_url}`);
                entityRow.metadata = {};
                entityRow.metadata.wikipedia_url = entity.metadata.wikipedia_url;
            }
            entityRowArr.push(entityRow);
        });
        let nlpRow = {
            id_str: tweet.id,
            sentiment_magnitude: sentiment.magnitude,
            sentiment_score: sentiment.score,
            entities: entityRowArr
        }
        if (utils.countWords(tweet.text) >= 20) {
            var catRowArr = [];
            const [classification] = await client.classifyText({ document });
            classification.categories.forEach(category => {
                if (category != null) {
                    var catRow = {};
                    catRow.name = category.name;
                    catRow.confidence = category.confidence;
                    // console.log(`Name: ${category.name}, Confidence: ${category.confidence}`);
                    catRowArr.push(catRow);
                }
            });
            if (catRowArr.length > 0) {
                nlpRow.categories = catRowArr
            }
        }

        nlpRows.push(nlpRow);
        console.log('GNLP Annotated -- ',tweet.category,' row', nlpRows.length, ' tweet ',nlpRow.id_str );
        utils.sleep(1000);
    }
    console.log('nlpRows ', nlpRows.length);
    // split array and insert 500 rows into BQ
    var len = nlpRows.length;
    var maxRowsToChuck = 10;
    if (len > maxRowsToChuck) {
        let bqIndex = (len - (len % maxRowsToChuck)) / maxRowsToChuck
        console.log('bqIndex ', bqIndex);
        while (bqIndex > 0) {
            fas_bq.insertRowsAsStream(config.nlp_bq_table, nlpRows.slice((bqIndex - 1) * maxRowsToChuck, bqIndex * maxRowsToChuck));
            if (bqIndex == 1)   {
                fas_bq.insertRowsAsStream(config.nlp_bq_table, nlpRows.slice((len-(len % maxRowsToChuck))-1, len));
            }
            bqIndex--;
        }
    }
    //fas_bq.insertRowsAsStream(config.nlp_bq_table, nlpRows);
}

module.exports = { annotateText, pullTweets };
