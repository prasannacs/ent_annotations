const { PubSub } = require('@google-cloud/pubsub');
const { v1 } = require('@google-cloud/pubsub');
const fas_bq = require('./fas-bq');
const config = require('../config.js');
const google_nlp = require('./google-nlp');
const watson_nlp = require('./watson-nlp');

const pubSubClient = new PubSub();

async function publishTweet(topicName, tweet, category) {
  let message = {
    id: tweet.id_str,
    text: tweet.text,
    category: category
  }
  publishMessage(topicName, JSON.stringify(message));
}

async function createTopic(topicName) {
  // Creates a new topic
  await pubSubClient.createTopic(topicName);
  console.log(`Topic ${topicName} created.`);
}

async function deleteTopic(topicName) {
  await pubSubClient.topic(topicName).delete();
  console.log(`Topic ${topicName} deleted.`);
}

async function createSubscription(topicName, subscriptionName) {
  // Creates a new subscription
  await pubSubClient.topic(topicName).createSubscription(subscriptionName);
  console.log(`Subscription ${subscriptionName} created.`);
}

async function deleteSubscription(subscriptionName) {
  // Deletes the subscription
  await pubSubClient.subscription(subscriptionName).delete();
  console.log(`Subscription ${subscriptionName} deleted.`);
}

async function publishMessage(topicName, message) {
  // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
  const dataBuffer = Buffer.from(message);

  try {
    const messageId = await pubSubClient.topic(topicName).publish(dataBuffer);
    //console.log(`Message ${messageId} published.`);
  } catch (error) {
    console.error(`Received error while publishing: ${error.message}`);
    process.exitCode = 1;
  }
}

const timeout = 60;

async function listenForMessages(dataSetName, topicName, subscriptionName, discriminator) {
  console.log('PubSub-listenForMessages-subscription ', subscriptionName);
  const subscription = pubSubClient.subscription(subscriptionName);
  var tweets = [];
  let messageCount = 0;
  const messageHandler = message => {
    // console.log(`Received message ${message.id}:`);
    // console.log(`\tData: ${message.data}`);
    // console.log(`\tAttributes: ${message.attributes}`);
    tweets.push(JSON.parse(message.data));
    messageCount += 1;
    // "Ack" (acknowledge receipt of) the message
    message.ack();
  };
  // Listen for new messages until timeout is hit
  subscription.on('message', messageHandler);

  setTimeout(() => {
    subscription.removeListener('message', messageHandler);
    console.log(`${messageCount} message(s) received.`);
    if (discriminator === 'GCP')
      google_nlp.annotateText(dataSetName, tweets);
    if (discriminator === 'WATSON')
      watson_nlp.analyze(dataSetName, tweets);

    deleteSubscription(subscriptionName);
    deleteTopic(topicName);
  }, timeout * 1000);
}

// Creates a client; cache this for further use.
const subClient = new v1.SubscriberClient();

async function synchronousPull(projectId, subscriptionName, maxMessagesToPull) {
  const formattedSubscription = subClient.subscriptionPath(
    projectId,
    subscriptionName
  );

  // The maximum number of messages returned for this request.
  // Pub/Sub may return fewer than the number specified.
  const request = {
    subscription: formattedSubscription,
    maxMessages: maxMessagesToPull,
  };

  // The subscriber pulls a specified number of messages.
  const [response] = await subClient.pull(request);
  //console.log('Subscription response ',response);

  // Process the messages.
  const ackIds = [];
  var tweets = [];

  for (const message of response.receivedMessages) {
    //console.log('Received Message :- ',message.message.data.toString());
    tweets.push(JSON.parse(message.message.data.toString()));
    ackIds.push(message.ackId);
  }

  // Insert into BQ
  //await insertResults(tweets,'cash');

  if (ackIds.length !== 0) {
    // Acknowledge all of the messages. You could also ackknowledge
    // these individually, but this is more efficient.
    const ackRequest = {
      subscription: formattedSubscription,
      ackIds: ackIds,
    };
    await subClient.acknowledge(ackRequest);
  }
  console.log('Done.');
  return tweets;
}

async function setupMsgInfra(requestBody) {
  var nlpObj = requestBody.naturalLanguage;
  var category = requestBody.fullArchiveSearch.category;
  var nlpSwitch = nlpObj.on;
  var dataSetName = requestBody.dataSet.dataSetName;
  return new Promise(function (resolve, reject) {
    if (nlpSwitch === false) {
      resolve('NLP setup skipped');
      return;
    }
    let topicName = config.nlp_topic + '_' + dataSetName + '_' + category;
    let subscriptionName = topicName + '_' + 'subscription';
    createTopic(topicName).then(() => {
      console.log('Topic created ', topicName);
      if (nlpObj.googleSvc === true) {
        createSubscription(topicName, subscriptionName).then(() => {
          console.log('Subscription created ', subscriptionName);
          //resolve(topicName);
          listenForMessages(dataSetName, topicName, subscriptionName, "GCP");
        });
      } if (nlpObj.watsonSvc === true) {
        let watsonSubscriptionName = topicName + '_' + 'watson_subs'
        createSubscription(topicName, watsonSubscriptionName).then(() => {
          console.log('Subscription created ', watsonSubscriptionName);
          //resolve(topicName);
          listenForMessages(dataSetName, topicName, watsonSubscriptionName, "WATSON");
        });
      }
      resolve(topicName);
    });

  })

}
module.exports = { listenForMessages, synchronousPull, publishTweet, createTopic, deleteTopic, createSubscription, deleteSubscription, setupMsgInfra };

