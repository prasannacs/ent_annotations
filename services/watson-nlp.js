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

async function analyze(tweets) {
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
            utils.sleep(1000);
        }
    }
    console.log('watsonRows ', watsonRows.length);
    // split array and insert 500 rows into BQ
    var len = watsonRows.length;
    var maxRowsToChuck = 10;
    if (len > maxRowsToChuck) {
        let bqIndex = (len - (len % maxRowsToChuck)) / maxRowsToChuck
        console.log('watson bqIndex ', bqIndex);
        while (bqIndex > 0) {
            fas_bq.insertRowsAsStream(config.watson_nlp_bq_table, watsonRows.slice((bqIndex - 1) * maxRowsToChuck, bqIndex * maxRowsToChuck));
            if (bqIndex == 1) {
                fas_bq.insertRowsAsStream(config.watson_nlp_bq_table, watsonRows.slice((len - (len % maxRowsToChuck)) - 1, len));
            }
            bqIndex--;
        }
    }
}

module.exports = { analyze, pullTweets };