const MONGO_DB_HOST="localhost";
const CURR_DB_NAME = "Currency_v2";
const AGG_DB_NAME = "CurrencyAggregate_v2";
const MONGO_DB_PORT = 27017;



var combine = require("currencycombine");
var MongoDB = require("mongodb");




var currList = combine.getCurrencyCodeList();
var statsList = combine.generateStatsList(currList);
const COLL_NAMES = combine.getCollectionNames();




var mongoDbHost = combine.argv(process.argv[2],MONGO_DB_HOST);
var mongoDbPort = combine.argv(process.argv[3],MONGO_DB_PORT);
var currDbName = combine.argv(process.argv[4],CURR_DB_NAME);
var aggDbName = combine.argv(process.argv[5],AGG_DB_NAME);


var MongoClient = MongoDB.MongoClient;
var Server = MongoDB.Server;

var mongoClient = new MongoClient(new Server(mongoDbHost,mongoDbPort));

console.log("DB Host: "+mongoDbHost+"\tCurr Db: "+currDbName+ "\tAgg Db: "+aggDbName);
    

 
console.log((new Date())+"  Bulk Resync started.. obtaining database links");

/*
 * Error Handler
 */

function hErr(err)
{
    if (err) {
        throw err;
    }
            
}

function hLogErr(err)
{
    if (err) {
        console.log(err);
    }
}


function hEndRaw()
{
    console.log("End Raw Table processing.");
}

function rawProcessor(err,raw)
{
    console.log("Processing started..");
    hErr(err);
    raw.count(function(err,count)
        {
            console.log("Raw Document counts: "+count);
            currDb.collection(COLL_NAMES.split,function(err,coSplit)
            {
                coSplit.drop(function(err,result)
                           {
                            hLogErr(err);
                            console.log("Split Collection dropped.");
                            var stream = raw.find().stream();
                           
                        
                            stream.on('data',
                                    function(item)
                                    {
                                        
                                        var rates = item["rates"];
                                        var docSet = {};
                                        docSet._id = item._id;
                                        for (var from in rates) {
                                            var doc = {};
                                            
                                            
                                            
                                            
                                                for(var to in rates)
                                                {
                                                    if (from!=to) {
                                                        var value = rates[to]/rates[from];
                                                        doc[to] = value;
                                                        updateStats(from,to,value);
                                                    }
                                                }
                                            
                                            
                                            docSet[from] = doc;
                                         
                                        }
                                        coSplit.insert(docSet,{safe:true}, function(err,result)
                                                       {
                                                            hErr(err);
                                                            count--;
                                                            
                                                            if (count == 0) {
                                                                console.log("Records written.");
                                                                writeStats();
                                                            }
                                                            
                                                       });
                                        
                                    }).on('end',hEndRaw);
                           });
            });
        }); 
    
    
}

       
function updateStats(from,to,value)
{
    var statsDoc = statsList[from][to];
    statsDoc.count = statsDoc.count+1;
    statsDoc.sum = statsDoc.sum+value;
    statsDoc.avg = statsDoc.sum*1/statsDoc.count;
    if (value > statsDoc.max) {
        statsDoc.max = value;
    }
    if (value < statsDoc.min){
        statsDoc.min = value;
    }
    
    statsList[from][to] = statsDoc;
}

function writeStats()
{
    currDb.collection(COLL_NAMES.stats, function(err, coStats)
                      {
                        hErr(err);
                        coStats.drop(function(err,result)
                                     {
                                        hLogErr(err);
                                        coStats.insert(statsList,{safe:true}, function(err,result)
                                        {
                                            console.log("Stats Doc written.");
                                            hErr(err);
                                            
                                            currDb.collection(COLL_NAMES.norm,function(err,coNorm)
                                            {
                                                coNorm.drop(function(err,result)
                                                           {
                                                            hLogErr(err);
                                                            console.log("Norm Collection dropped.");
                                                            
                                                            currDb.collection(COLL_NAMES.split,function(err,coSplit)
                                                                              {
                                                                                var stream = coSplit.find().stream();
                                                                               
                                                                                var count = statsList["USD"]["GBP"].count;
                                                                                console.log("Split Document count: "+count);
                                                                                stream.on('data',
                                                                                        function(item)
                                                                                        {
                                                                                            
                                                                                            
                                                                                            var docSet = {};
                                                                                            
                                                                                            docSet._id = item._id;
                                                                                            for (var from in item) {
                                                                                                if (from !="_id") {
                                                                                                 
                                                                                                    var normDoc = {};
                                                                                                    var baseDoc = item[from];
                                                                                                    for(var to in baseDoc)
                                                                                                    {
                                                                                                       
                                                                                                        
                                                                                                       var statsDoc = statsList[from][to];
                                                                                                       normDoc[to]=baseDoc[to]*1/statsDoc.max;
                                                                                                       
                                                                                                    }
                                                                                                    docSet[from] = normDoc;
                                                                                                }
                                                                                             
                                                                                            }
                                                                                            
                                                                                            coNorm.insert(docSet,{safe:true}, function(err,result)
                                                                                                           {
                                                                                                                
                                                                                                                hErr(err);
                                                                                                                
                                                                                                                count--;
                                                                                                                
                                                                                                                if (count == 0) {
                                                                                                                    console.log((new Date())+"  Normalised Records written.");
                                                                                                                  
                                                                                                                }
                                                                                                                
                                                                                                           });
                                                                                            
                                                                                        }).on('end',hEndRaw);
                                                                              });
                                                           });
                                            });
                                        });
                                     });
                        
                      });
}
var currDb = null;
var aggDb = null;

mongoClient.open(function(err,client)
                 {
                    
                    hErr(err);
                    currDb = client.db(currDbName);
                    aggDb = client.db(aggDbName);
                   
                    
                    currDb.collection(COLL_NAMES.raw,rawProcessor);
                 });


