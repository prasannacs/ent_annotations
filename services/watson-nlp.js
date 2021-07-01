const { IamAuthenticator } = require('ibm-watson/auth');
const NaturalLanguageUnderstandingV1 = require('ibm-watson/natural-language-understanding/v1.js');

const config = require('../config.js');
const pub_sub = require('./pub-sub.js');
const fas_bq = require('./fas-bq');
const utils = require('./utils');


const naturalLanguageUnderstanding = new NaturalLanguageUnderstandingV1({
    version: '2021-03-25',
    authenticator: new IamAuthenticator({
        apikey: config.watson.nlp.apikey,
    }),
    serviceUrl: config.watson.nlp.url,
});


async function pullTweets() {
    console.log('subscription name ', config.nlp_messages_to_pull);
    let tweets = await pub_sub.synchronousPull(config.gcp_projectId, config.nlp_subscription, config.nlp_messages_to_pull);

    console.log('Tweets pulled -- ', tweets.length);
    if (tweets != null && tweets.length > 0) {
        annotateText(tweets);
    }
}

async function analyze(dataSetName, tweets) {
    var watsonRows = [];
    for (let tweet of tweets) {
        if (utils.countWords(tweet.text) >= 5) {
            const analyzeParams = {
                'text': tweet.text,
                'features': {
                    'entities': {
                        'emotion': true,
                        'sentiment': true,
                        'limit': 2,
                    },
                    'keywords': {
                        'emotion': true,
                        'sentiment': true,
                        'limit': 2,
                    },
                },
            };
            await naturalLanguageUnderstanding.analyze(analyzeParams)
                .then(analysisResults => {
                    //console.log('Watson processing: ',JSON.stringify(analysisResults, null, 2));
                    if (analysisResults != undefined) {
                        let watsonRow = {
                            id_str: tweet.id,
                            result: analysisResults.result
                        }
                        watsonRows.push(watsonRow);
                        console.log('Watson NLP Annotated -- ', tweet.category, ' row', watsonRows.length, ' tweet ', watsonRow.id_str);
                    }

                })
                .catch(err => {
                    console.log('error:', err);
                });
                if( watsonRows.length > 9 ) {
                    fas_bq.insertRowsAsStream(dataSetName, config.watson_nlp_bq_table, watsonRows);
                    watsonRows = []
                }
            utils.sleep(1000);
        }
    }
    console.log('Remaining watsonRows ', watsonRows.length);
    fas_bq.insertRowsAsStream(dataSetName, config.watson_nlp_bq_table, watsonRows);
}

module.exports = { analyze, pullTweets };
