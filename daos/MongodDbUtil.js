var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;

var config = require('../config/config.' + process.env.NODE_ENV);
var mongoConfig = config.dbConfig;
var url = mongoConfig.url;

//Cache the mongodb connection
var dbCache = {};

module.exports.connectPromise = function () {
    return new Promise((resolve, reject) => {
        MongoClient.connect(url).then(db => {
            console.log("Connection with mongodb successful");
            dbCache.db = db;
            resolve();
        }).catch(err => {
            console.log("Error while connecting to Mongo DB " + err);
            dbCache = {};
            reject(err);
        });
    });
};

module.exports.getDb = function () {
    return dbCache.db;
}

module.exports.getMongodb = function () {
    return mongodb;
}

module.exports.ObjectID = mongodb.ObjectId;