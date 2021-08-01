const config = require('../config.js');

function sleep(milliseconds) {
    const date = Date.now();
    let currentDate = null;
    do {
        currentDate = Date.now();
    } while (currentDate - date < milliseconds);
}

function countWords(str) {
    str = str.replace(/(^\s*)|(\s*$)/gi, "");
    str = str.replace(/[ ]{2,}/gi, " ");
    str = str.replace(/\n /, "\n");
    return str.split(' ').length;
}

function getEngagementsSQL(reqBody) {
    let tableName = reqBody.dataSet.dataSetName + '.' + config.bq.table.fas_results;
    return `SELECT
    id_str, text, user, lang, category, subcategory, created_at, quote_count, reply_count, retweet_count, favorite_count, tweet_url 
    FROM `+ tableName +` 
    WHERE category = '`+ reqBody.fullArchiveSearch.category +`' AND subcategory = '`+ reqBody.fullArchiveSearch.subCategory +`'  
    ORDER BY favorite_count, retweet_count, reply_count, quote_count desc 
    LIMIT `+reqBody.followers.maxUsers;
}

function getFollowsSQL(reqBody) {
    let tableName = reqBody.dataSet.dataSetName + '.' + config.bq.table.follows;
    return `SELECT
    user_id, name, username 
    FROM `+ tableName +` 
    WHERE category = '`+ reqBody.fullArchiveSearch.category +`' AND subcategory = '`+ reqBody.fullArchiveSearch.subCategory +`'  
    LIMIT `+reqBody.followers.maxUsersProfiles;
}

module.exports = { sleep, countWords, getEngagementsSQL, getFollowsSQL };