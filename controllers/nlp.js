const express = require("express");
const fs = require('fs');
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const config = require('../config.js');
const nlp_svcs = require('.././services/google-nlp.js');
const watson_svcs = require('.././services/watson-nlp.js');

const pub_sub = require('../services/pub-sub.js');

const router = express.Router();

router.get("/", function (req, res) {
  console.log(req.body);
  pub_sub.listenForMessages(req.body.topicName, req.body.subscriptionName, req.body.discriminator);
  //watson_svcs.analyze();
  res.send("Twitter Enterprise API NLP Application");
});

module.exports = router;
