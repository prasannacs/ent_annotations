const express = require("express");
const fs = require('fs');
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const config = require('../config.js');
const nlp_svcs = require('.././services/nlp.js');

const router = express.Router();

router.get("/", function (req, res) {
    nlp_svcs.pullTweets();
  res.send("Twitter Enterprise API NLP Application");
});

module.exports = router;
