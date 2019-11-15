// handler.js
'use strict';

const express = require('express');
const serverless = require('serverless-http');
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const csv = require('csvtojson');
const AWS = require('aws-sdk');


const mongoConnStr = process.env.MongoDbUrl || 'mongodb://localhost';
const S3 = new AWS.S3();


const getWinnersFromArray = (ArrayData) => {
        const currenArrayLength = ArrayData.length;
        const approxHalfength = parseInt(currenArrayLength/2);
        //console.log('Total Array Lenhth: '+ currenArrayLength);
        //console.log('approxHalfength Array Lenhth: '+ approxHalfength);
        if(approxHalfength < 10){
        const dataSet =  ArrayData.sort(() => Math.random() - Math.random()).slice(0, 3);
        return dataSet;
        }else{
            const dataSet =  ArrayData.sort(() => Math.random() - Math.random()).slice(0, approxHalfength);
            return getWinnersFromArray(dataSet);
        }
}



const client = new MongoClient(mongoConnStr, {
    useNewUrlParser: true,
});
let db;

const createConn = async () => {
    await client.connect();
    db = client.db('giveaway');
};

const app = express();

app.get('/get-winners', async function (req, res) {
    const params = {
        Bucket: 'sandip-youtube-giveaway',
        Key: 'participants.csv'
      };
  const stream = S3.getObject(params).createReadStream();
  const json = await csv().fromStream(stream);
  //console.log(json);
  const winners = getWinnersFromArray(json);
  console.log('Winners');
  console.log(winners);
  //process.exit();

  if (!client.isConnected()) {
    // Cold start or connection timed out. Create new connection.
    try {
        await createConn();
    } catch (e) {
        res.status(500).json({
            error: e.message,
        });
        return;
    }
}

 // Connection ready.  insert the winner data in the database collection
try {
     const WinnersCollection = db.collection('winners');
    res.json({
        winners: winners,
        mongoDbResult: await WinnersCollection.insertMany(winners),
    });
    return;
} catch (e) {
    res.status(500).send({
        error: e.message,
    });
    return;
}

});


module.exports = {
    app,
    getWinners: serverless(app),
};
