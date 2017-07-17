var Twitter = require('twitter');
var MongoClient = require('mongodb').MongoClient;


var data = {
  consumer_key: '9BS8wxBjyyyvZKMc0priJ2OB7',
  consumer_secret: 'XmGgmsEcjYOWPgAPTAOfJydcJ6LheDQJuNG4513vXE2M4GMCvN',
  access_token_key: '807686596979986432-qxEZpvCjqZ9UKYxLzJXpRFxMPFypvXp',
  access_token_secret: 'U2CDd0PNjgegu0fmsh7JB0SUcWpEvbbE7nCuruBUmqWKU'
};
var client = new Twitter(data);
var SILENT = false;
var INTERVAL = 30*60*1000;

var SERVER_HOST = "192.168.0.19";
var DB = "Currency_v2";
var COLL = "Raw";
var URL = "mongodb://"+SERVER_HOST+":27017/"+DB;

var prevStatus = null,status = null;
var prevGBP=0, prevINR=0, prevEUR=0, prevNGN=0, GBP=0, EUR=0, INR=0, NGN=0;
const DELTA_PER = 2;
setInterval(function() {
    MongoClient.connect(URL, function(error, db)
                    {
                       if(error)
                       {
                            console.error("Error",error);
                            return;
                       }
                       
                       db.collection(COLL, function(error, coll)
                                     {
                                        if(error)
                                        {
                                            console.error("Error",error);
                                            return;
                                        }
                                        prevStatus = status;
                                        prevEUR = EUR;
                                        prevGBP = GBP;
                                        prevINR = INR;
                                        coll.find().sort({"_id":-1}).limit(1).forEach(function(doc){
											if (doc!==null && doc.rates!==null) {
                                                status = "1 USD = "+(GBP = doc.rates.GBP)+" GBP; "+(INR = doc.rates.INR)+" INR;  "+(EUR = doc.rates.EUR)+" EUR;  "+(NGN = doc.rates.NGN)+" NGN;";
                                                if (status!==null && prevStatus!==null && prevStatus !== status) {
                                                    tweet(status);
                                                    delta(prevEUR,EUR,"EUR - Euro");
                                                    delta(prevGBP,GBP,"GBP - UK");
                                                    delta(prevINR,INR,"INR - India");
                                                    delta(prevNGN,NGN,"NGN - Nigeria");
                                                }
                                                else
                                                {
                                                    console.log("Skipped one.",status,prevStatus);
                                                }
                                    
                                                
                                            }
                                            
                                            db.close();
										});
                                          
                                     });
                    });
}, INTERVAL);


function delta(prev,curr, currCode) {
    if (prev>0 && curr!==null) {
        var per_change = (curr-prev)*100/prev;
        if (per_change>=DELTA_PER) {
            tweet("ALERT! "+currCode+" |rising| against USD by (%): "+per_change);
        }
        if(per_change<=-DELTA_PER) {
            tweet("ALERT! "+currCode+" |falling| against USD by (%): "+per_change);
        }
    }
    
    return;
}
function tweet(statusText) {
    //Tweet to Twitter
    if (SILENT) {
        console.log(statusText);
        return;
    }
            client.post('statuses/update', {status : statusText}, function(error, tweet, response) {
                    if (error) {												
                        console.error("Error: ",error);
                        return;
                    }                                                                                                                         
            });
    //code
}
