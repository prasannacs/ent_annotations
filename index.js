const express = require('express');
const bodyParser = require('body-parser')
const cors = require('cors')
const search = require('./controllers/search')
const nlp = require('./controllers/nlp')

const app = express();
const port = process.env.PORT || 4040;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use(cors());
app.options('*', cors()) 
app.post('*', cors()) 
app.use('/search',search);
app.use('/nlp',nlp);

app.listen(port, ()=>   {
    console.log("App listening on port",port)
})