const language = require('@google-cloud/language');
const config = require('../config.js');
const pub_sub = require('./pub_sub');

async function pullTweets() {
    console.log('subscription name ',config.nlp_subscription);
    pub_sub.synchronousPull(config.gcp_projectId, config.nlp_subscription, 10);
}

async function annotateText() {
    const client = new language.LanguageServiceClient();

    const text = 'I’m grateful for Jay’s vision, wisdom, and leadership. I knew TIDAL was something special as soon as I experienced it, and I’m inspired to work with him. He willl now help lead our entire company, including Seller and the Cash App, as soon as the deal closes. ';

    const document = {
        content: text,
        type: 'PLAIN_TEXT',
    };

    const features = {
        "extractSyntax": true,
        "extractEntities": true,
        "extractDocumentSentiment": true,
        "extractEntitySentiment": true
    }

    const [result] = await client.annotateText({ document: document, features: features });
    // const [classification] = await client.classifyText({document});

    const sentiment = result.documentSentiment;
    const entities = result.entities;


    console.log(`Text: ${text}`);
    console.log(`result: ${result}`);
    console.log(`Sentiment score: ${sentiment.score}`);
    console.log(`Sentiment magnitude: ${sentiment.magnitude}`);
    console.log('Entities:');
    entities.forEach(entity => {
        console.log(entity.name);
        console.log(` - Type: ${entity.type}, Salience: ${entity.salience}`);
        if (entity.metadata && entity.metadata.wikipedia_url) {
            console.log(` - Wikipedia URL: ${entity.metadata.wikipedia_url}`);
        }

    });
    // console.log('Categories:', classification);
    // classification.categories.forEach(category => {
    //     console.log(`Name: ${category.name}, Confidence: ${category.confidence}`);
    // });

}

module.exports = { annotateText, pullTweets };
