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

async function annotateText(dataSetName, tweets) {
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
        var nlpRow = {};
        // const [result] = await client.annotateText({ document: document, features: features }); 
        await client.annotateText({ document: document, features: features })
            .then( result => {  
                //console.log('result - GNLP ',JSON.stringify(result));
                let sentiment = result[0].documentSentiment;
                let entities = result[0].entities;
                var entityRowArr = [];
        
                entities.forEach(entity => {
                    var entityRow = {};
                    entityRow.name = entity.name;
                    entityRow.type = entity.type;
                    entityRow.salience = entity.salience;
                    if (entity.metadata && entity.metadata.wikipedia_url) {
                        entityRow.metadata = {};
                        entityRow.metadata.wikipedia_url = entity.metadata.wikipedia_url;
                    }
                    entityRowArr.push(entityRow);
                });
                nlpRow = {
                    id_str: tweet.id,
                    sentiment_magnitude: sentiment.magnitude,
                    sentiment_score: sentiment.score,
                    entities: entityRowArr
                }
            })
            .catch(err => {
                console.log('error:', err);
                //return;
            })

        if (utils.countWords(tweet.text) >= 20) {
            var catRowArr = [];
            const [classification] = await client.classifyText({ document });
            classification.categories.forEach(category => {
                if (category != null) {
                    var catRow = {};
                    catRow.name = category.name;
                    catRow.confidence = category.confidence;
                    catRowArr.push(catRow);
                }
            });
            if (catRowArr.length > 0) {
                nlpRow.categories = catRowArr
            }
        }

        nlpRows.push(nlpRow);
        console.log('Google NLP Annotated -- ',tweet.category,' row', nlpRows.length, ' tweet ',nlpRow.id_str );
        if( nlpRows.length > 9 )   {
            fas_bq.insertRowsAsStream(dataSetName, config.nlp_bq_table,nlpRows);
            nlpRows = []
        }
        utils.sleep(1000);
    }
    console.log('Insert remaining nlpRows ', nlpRows.length);
    if( nlpRows.length > 0)
        fas_bq.insertRowsAsStream(dataSetName, config.nlp_bq_table,nlpRows);
}

module.exports = { annotateText, pullTweets };
