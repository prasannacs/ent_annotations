const express = require("express");
const fs = require('fs');
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const config = require('../config.js');
const nlp_svcs = require('.././services/google-nlp.js');
const pub_sub = require('../services/pub-sub.js');

const router = express.Router();

router.get("/", function (req, res) {
    //nlp_svcs.pullTweets();
    pub_sub.listenForMessages(config.nlp_subscription);
  res.send("Twitter Enterprise API NLP Application");
});

module.exports = router;
