const MONGO_DB_HOST="localhost";
const CURR_DB_NAME = "Currency_v2";
const AGG_DB_NAME = "CurrencyAggregate_v2";
const MONGO_DB_PORT = 27017;
const TIMER_DISABLED = 0;
const AGG_WORKER_COUNT = 4;
const START_TIME = new Date().getTime();

var combine = require("currencycombine");
var MongoDB = require("mongodb");
var events = require("events");
var tools = require("tools");
var fork = require("child_process").fork;




var currList = combine.getCurrencyCodeList();
var statsCollList = combine.getStatsCodeList();
var statsList = combine.generateStatsList(currList);
var normCollList = combine.getNormCodeList();
const COLL_NAMES = combine.getCollectionNames();




var mongoDbHost = tools.argv(process.argv[2],MONGO_DB_HOST);
var mongoDbPort = tools.argv(process.argv[3],MONGO_DB_PORT);
var currDbName = tools.argv(process.argv[4],CURR_DB_NAME);
var aggDbName = tools.argv(process.argv[5],AGG_DB_NAME);




var MongoClient = MongoDB.MongoClient;
var Server = MongoDB.Server;

var mongoClient = new MongoClient(new Server(mongoDbHost,mongoDbPort));

console.log("DB Host: "+mongoDbHost+"\tCurr Db: "+currDbName+ "\tAgg Db: "+aggDbName);
    

 
console.log((new Date())+"  Bulk Resync started.. obtaining database links");

function Drop(_db) {
                        this.db = _db;
                        events.EventEmitter.call(this);
                        this.dropAllCurrencies = function (currencyList) {
                            var count = 0;
                            var self = this;
                            
                            for(var i=0;i<currencyList.length;i++)
                            {
                                
                                currDb.dropCollection(currencyList[i],function(err,result)
                                                      {
                                                         hLogErr("DROP",err);
                                                        count++;
                                                        if (count == currencyList.length) {
                                                            console.log("Dropped all");
                                                            self.emit('dropped-all');
                                                            
                                                        }
                                                      });
                            }
                            
                        };
                    };
Drop.prototype = Object.create(events.EventEmitter.prototype);

/*
 * Error Handler
 */
function hErr(err)
{
    if (err) {
        
        throw err;
    }
            
}



function hLogErr(category,err)
{
    if (err) {
        console.log(category+"\n"+err);
    }
}






function rawProcessor(err,raw)
{
    
    console.log("Processing started..");
    hErr(err);
    var rawDrop = new Drop(currDb);
    rawDrop.on('dropped-all', function()
                    {
                        
                         
            
                            
            
                            hLogErr("RAW",err);
                            console.log("Split Collection dropped.");
                            var stream = raw.find().stream();
                            
                        
                            stream.on('data',
                                    function(item)
                                    {
                                        
                                        var rates = item["rates"];
                                       
                                        for (var from in rates) {
                                            var doc = {};
                                            doc._id = item._id;
                                            
                                            
                                            
                                                for(var to in rates)
                                                {
                                                    if (from!=to) {
                                                        var value = rates[from]/rates[to];
                                                        doc[to] = value;
                                                        updateStats(from,to,value);
                                                    }
                                                }
                                            
                                            
                                            currDb.collection(from).insert(doc,{safe:true}, function(err,result)
                                                       {
                                                            hLogErr(err);
                                                            
                                                            
                                                       });
                                         
                                        }
                                        
                                        
                                    }).on('end',function ()
                                        {
                                            
                                            console.log("End Raw Table processing.");
                                            writeStats();
                                            
                                        });
                           
                    }).dropAllCurrencies(currList);
    
   
        
    
    
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
    
    statsDoc.range = statsDoc.max - statsDoc.min;
    statsList[from][to] = statsDoc;
}

function writeStat(completed, key, statsDoc)
{   
    currDb.collection(COLL_NAMES.stats).insert(statsDoc,{safe:true},function(err,result)
                                                                          {
                                                                            hLogErr("STATS",err);
                                                                            completed.push(1);;
                                                                            
                                                                            if (completed.length == currList.length) {
                                                                                console.log("Stats done...");
                                                                                normalise();
                                                                            }
                                                                           
                                                                          });
}
function writeStats()
{
    console.log("Starting Stats Writing...");
    var statsDrop = new Drop(currDb);
    statsDrop.on('dropped-all',
                                    function()
                                     {
                                        console.log("Dropped stats...");
                                        var completed = [];
                                        for(var key in statsList)
                                        {  var statsDoc = statsList[key];
                                            statsDoc._id = "STATS_"+key;
                                            
                                           writeStat(completed, key,statsDoc);     
                                        }
                                        }).dropAllCurrencies([COLL_NAMES.stats]);
                             

}


function normalise(){
    console.log("Starting Norm Writing...");
    var normDrop = new Drop(currDb);
    var completed = [];
    normDrop.on('dropped-all',
                function()
                {
                        console.log("Norm Collection dropped.");
                        
                        console.log("Processing docs: ",statsList["USD"]["GBP"].count*currList.length);
                        for (var i = 0; i<currList.length;i++) {
                            
                                normaliseOne(completed,currList[i]);
                                
                        }
                }).dropAllCurrencies(normCollList);
}

function normaliseOne(completed,curr)
{
    currDb.collection("NORM_"+curr,function(err,coNorm)
    {
                                               
        hLogErr("NORM",err);
                        
                           
        currDb.collection(curr,function(err,coCurr)
                          {
                            var stream = coCurr.find().stream();
                           
                            
                            var count = statsList["USD"]["GBP"].count;
                            
                            stream.on('data',
                                    function(item)
                                    {
                                    
                                        
                                            
                                             
                                                var normDoc = {};
                                                normDoc._id = item._id;
                                                var statsDoc = statsList[curr];
                                                
                                                for(var to in item)
                                                {
                                                 if (to!="_id") {
                                                     
                                                   
                                                   normDoc[to]=(item[to]-statsDoc[to].min)/(statsDoc[to].max-statsDoc[to].min);
                                                 }
                                                   
                                                }
                                                
                                            
                                         
                                        
                                        
                                        coNorm.insert(normDoc,{safe:true}, function(err,result)
                                                       {
                                                            
                                                            hErr(err);
                                                            
                                                            count--;
                                                            
                                                            if (count == 0) {
                                                              completed.push(curr);  
                                                              
                                                            }
                                                            
                                                            if (count < 0) {
                                                              console.log("Count in statistics less than data count, possible data corruption. Please resync from Raw data again");  
                                                            }
                                                            if (completed.length == currList.length) {
                                                                aggregate();
                                                            }
                                                       });
                                        
                                    });
                          });
                       
           });
}


function aggregate()
{
    var SRC = "mongodb://"+MONGO_DB_HOST+":"+MONGO_DB_PORT+"/"+CURR_DB_NAME;
    var TGT = "mongodb://"+MONGO_DB_HOST+":"+MONGO_DB_PORT+"/"+AGG_DB_NAME;
    var batchSize = currList.length*1/AGG_WORKER_COUNT;
    var activeTokens=[];
    var list = [];
    var batchCount = 0;
 
    for(var i=0;i<currList.length;i++)
    {
        list.push(currList[i]);
        if (list.length>=batchSize) {
            batchCount++;
            activeTokens.push(batchCount);
            
            var cp = fork("./aggregate",[JSON.stringify(list),true,SRC,TGT,batchCount]);
            
            cp.on('exit', function()
                  {
                    activeTokens.pop();
                    if (activeTokens.length == 0) {
                        console.log("Finished","\nTotal Time taken (min): ",((new Date()).getTime()-START_TIME)/60000);
                                                                    process.exit();
                    }
                  });
            list = [];
        }
        
    }
    
      if (list.length> 0) {
            batchCount++;
            console.log(batchCount,list);
            activeTokens.push(batchCount);
            
            var cp = fork("./aggregate",[list,true,SRC,TGT,batchCount]);
            
            cp.on('exit', function()
                  {
                    activeTokens.pop();
                    if (activeTokens.length == 0) {
                        console.log("Finished","\nTotal Time taken (min): ",((new Date()).getTime()-START_TIME)/60000);
                                                                    process.exit();
                    }
                  });
        }
    
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




