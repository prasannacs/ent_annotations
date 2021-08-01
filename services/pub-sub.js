const { PubSub } = require('@google-cloud/pubsub');
const { v1 } = require('@google-cloud/pubsub');
const fas_bq = require('./fas-bq');
const config = require('../config.js');
const google_nlp = require('./google-nlp');
const watson_nlp = require('./watson-nlp');
const vertex_svcs = require('./vertexClassification');

const utils = require('./utils');
const follower_svcs = require('./followers');

const pubSubClient = new PubSub();

async function publishTweet(topicName, tweet, category) {
  let message = {
    id: tweet.id_str,
    text: tweet.text,
    category: category,
    userId: tweet.user.id,
    userName: tweet.user.name
  }
  publishMessage(topicName, JSON.stringify(message));
}

async function publishUserProfile(topicName, profile, category) {
  let message = {
    userId: profile.user_id,
    name: profile.name,
    userName: profile.username,
    category: profile.category,
    subCategory: profile.subcategory
  }
  publishMessage(topicName, JSON.stringify(message));
}

async function publishTweets(tweets, category, topicName) {
  console.log('publishing tweets ', tweets.length);
  if (tweets === null || tweets.length < 1) {
    console.log("Cannot publish empty Tweets array or Category is empty")
    return;
  }
  tweets.forEach(function (tweet, index) {
    publishTweet(topicName, tweet, category);
  });
}

async function publishUserProfiles(userProfiles, category, topicName) {
  console.log('publishing userProfiles ', userProfiles.length);
  if (userProfiles === null || userProfiles.length < 1) {
    console.log("Cannot publish empty userProfiles array or Category is empty")
    return;
  }
  userProfiles.forEach(function (userProfile, index) {
    publishUserProfile(topicName, userProfile, category);
  });
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
  await pubSubClient.subscription(subscriptionName).delete();
  console.log(`Subscription ${subscriptionName} deleted.`);
}

async function publishMessage(topicName, message) {
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

async function listenForMessages(requestBody, discriminator) {
  let dataSetName = requestBody.dataSet.dataSetName;
  let topicName = requestBody.naturalLanguage.topicName;
  let subscriptionName;

  if (discriminator === 'GCP')
    subscriptionName = requestBody.naturalLanguage.google.subscriptionName;
  if (discriminator === 'WATSON')
    subscriptionName = requestBody.naturalLanguage.watson.subscriptionName;
  if (discriminator === 'CXM')
    subscriptionName = requestBody.machineLearning.cxm.subscriptionName;

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
    if (discriminator === 'GCP') {
      let maxTweetsToAnnotate = requestBody.naturalLanguage.google.maxTweetsToAnnotate;
      if (maxTweetsToAnnotate < tweets.length)
        tweets = tweets.slice(0, maxTweetsToAnnotate);
      google_nlp.annotateText(dataSetName, tweets);
    }
    if (discriminator === 'WATSON') {
      let maxTweetsToAnnotate = requestBody.naturalLanguage.watson.maxTweetsToAnnotate;
      if (maxTweetsToAnnotate < tweets.length)
        tweets = tweets.slice(0, maxTweetsToAnnotate);
      watson_nlp.analyze(dataSetName, tweets);
    }
    if (discriminator === 'CXM') {
      let maxTweetsToAnnotate = requestBody.machineLearning.cxm.maxTweetsToAnnotate;
      if (maxTweetsToAnnotate < tweets.length)
        tweets = tweets.slice(0, maxTweetsToAnnotate);
      vertex_svcs.predictTextClassification(dataSetName, tweets, config.mlModel.cxm.vertex_cxm_endpointId);
    }

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
  const request = {
    subscription: formattedSubscription,
    maxMessages: maxMessagesToPull,
  };

  const [response] = await subClient.pull(request);
  const ackIds = [];
  var tweets = [];

  for (const message of response.receivedMessages) {
    tweets.push(JSON.parse(message.message.data.toString()));
    ackIds.push(message.ackId);
  }

  // Insert into BQ
  //await insertResults(tweets,'cash');

  if (ackIds.length !== 0) {
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
  let mlObj = requestBody.machineLearning;
  let mlSwitch = mlObj.on;
  var category = requestBody.fullArchiveSearch.category;
  var nlpSwitch = nlpObj.on;
  var dataSetName = requestBody.dataSet.dataSetName;
  return new Promise(function (resolve, reject) {
    if (nlpSwitch === false && mlSwitch === false) {
      resolve('NLP, ML setup skipped');
      return;
    }
    let topicName = config.nlp_topic + '_' + dataSetName + '_' + category;
    let subscriptionName = topicName + '_' + 'subscription';
    createTopic(topicName).then(() => {
      console.log('Topic created ', topicName);
      requestBody.naturalLanguage.topicName = topicName;
      requestBody.machineLearning.topicName = topicName;
      if (nlpObj.google.googleSvc === true && nlpSwitch === true) {
        createSubscription(topicName, subscriptionName).then(() => {
          console.log('Subscription created ', subscriptionName);
          requestBody.naturalLanguage.google.subscriptionName = subscriptionName;
          listenForMessages(requestBody, 'GCP');
          resolve(topicName);
        });
      } if (nlpObj.watson.watsonSvc === true && nlpSwitch === true) {
        let watsonSubscriptionName = topicName + '_' + 'watson_subs'
        createSubscription(topicName, watsonSubscriptionName).then(() => {
          console.log('Subscription created ', watsonSubscriptionName);
          requestBody.naturalLanguage.watson.subscriptionName = watsonSubscriptionName;
          listenForMessages(requestBody, 'WATSON');
          resolve(topicName);
        });
      }
      if (mlObj.cxm.vertexSvc === true) {
        let cxmSubscriptionName = topicName + '_' + 'cxm_subs'
        createSubscription(topicName, cxmSubscriptionName).then(() => {
          console.log('Subscription created ', cxmSubscriptionName);
          requestBody.machineLearning.cxm.subscriptionName = cxmSubscriptionName;
          listenForMessages(requestBody, 'CXM');
          resolve(topicName);
        });
      }
      //resolve(topicName);
    });

  })

}

async function subscribeWithFlowControlSettings(requestBody, maxInProgress, discriminator) {
  const subscriberOptions = {
    flowControl: {
      maxMessages: maxInProgress,
    },
  };
  // References an existing subscription.
  // Note that flow control settings are not persistent across subscribers.
  const subscription = pubSubClient.subscription(
    requestBody.followers.subscriptionName,
    subscriberOptions
  );

  console.log(
    `Subscriber to subscription ${subscription.name} is ready to receive messages at a controlled volume of ${maxInProgress} messages.`
  );

  const messageHandler = message => {
    // console.log(`Received message: ${message.id}`);
    // console.log(`\tData: ${message.data}`);
    // console.log(`\tAttributes: ${message.attributes}`);
    let userPayload = JSON.parse(message.data.toString());
    if (discriminator === 'follows') {
      console.log('followers message ', userPayload);
      follower_svcs.followersGraph(userPayload.userId, requestBody).then((followers) => {
        console.log('Sleeping for V2 followers rate limit ');
        utils.sleep(5000);
        message.ack();
      });
    }
    if (discriminator === 'userProfiles') {
      console.log('userProfiles message ', userPayload);
      follower_svcs.getUserProfiles(userPayload.userId, requestBody).then((fasTweets) => {
        console.log('Sleeping for User profiles rate limit ');
        utils.sleep(3000);
        message.ack();
      });
    }
  };

  subscription.on('message', messageHandler);

  setTimeout(() => {
    console.log(discriminator,' subscription closed');
    subscription.close();
    deleteSubscription(requestBody.followers.subscriptionName);
    deleteTopic(requestBody.followers.topicName);
    //follower_svcs.getUserProfiles(requestBody);
  }, timeout * 10000);
}

module.exports = {
  listenForMessages, synchronousPull, publishTweet, createTopic, deleteTopic,
  createSubscription, deleteSubscription, setupMsgInfra, publishTweets, subscribeWithFlowControlSettings, publishUserProfiles
};

