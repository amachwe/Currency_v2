/*
 * Delta Split Service 
 */

var mongoClient=require('mongodb').MongoClient;
var tools = require("tools");
var combine = require("currencycombine");
var events = require("events");


const MONGO_DB_URL="mongodb://localhost:27017/Currency_v2";
const MONGO_SRC_DB = "Currency_v2";
const COLL_NAMES = combine.getCollectionNames();
const currList = combine.getCurrencyCodeList();

var mongoDbUrl = tools.argv(process.argv[2],MONGO_DB_URL);
var mongoSrcColl = tools.argv(process.argv[3],MONGO_SRC_DB);




var currDb = null;


var statsList = {};
process.on('message', function(msg)
           {
                var req = msg.type;
                
                if (req == "PROCESS") {
                 
                    var item = msg.data;
            
                    mongoClient.connect(MONGO_DB_URL,function(err,db)
                    {
                      tools.err(err);
                        
                           currDb = db;
                    
                    var stream = db.collection(COLL_NAMES.stats).find().stream();
                    stream.on('data',function(data)
                    {
                        var from = data._id.split("_")[1];
                        statsList[from] = {};
                        for(var to in data)
                        {
                            if (to!="_id" && to!=from) {
                                statsList[from][to] = data[to];
                            }
                        }
                        
                    }).on('end', function(){
                        db.collection(MONGO_SRC_DB, function(err,src)
                                   {
                                    
                                        tools.err(err);
                                        
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
                                            
                                            
                                            db.collection(from).insert(doc,{safe:true}, function(err,result)
                                                       {
                                                            if (err) console.log(err);
                                                            
                                                            
                                                       });
                                         
                                        }
                                        
                                        console.log("End Raw Table processing.");
                                        writeStats();
                                    
                                     

                                   });
                    });
                    });
                }
                
                    
                   
        });

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
                                                                            if (err) console.log("STATS",err);
                                                                            completed.push(1);;
                                                                            
                                                                            if (completed.length == currList.length) {
                                                                                console.log("Stats done...");
                                                                                currDb.close();
                                                                                process.exit();
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


function Drop(_db) {
                        this.db = _db;
                        events.EventEmitter.call(this);
                        this.dropAllCurrencies = function (currencyList) {
                            var count = 0;
                            var self = this;
                            
                            for(var i=0;i<currencyList.length;i++)
                            {
                               
                                self.db.dropCollection(currencyList[i],function(err,result)
                                                      {
                                                         if (err) console.log("DROP",err);
                                                       
                                                        count++;
                                                        if (count == currencyList.length) {
                                                            console.log("Dropped all");
                                                            self.emit('dropped-all',count);
                                                            
                                                        }
                                                      });
                            }
                            
                        };
                    };
Drop.prototype = Object.create(events.EventEmitter.prototype);