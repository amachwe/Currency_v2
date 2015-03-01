const MONGO_DB_HOST="localhost"; const CURR_DB_NAME = "Currency_v2";

const MONGO_DB_PORT = 27017; const TIMER_DISABLED = 0; const AGG_WORKER_COUNT =
4; const START_TIME = new Date().getTime(); const DO_SPLIT = "false";

var combine = require("currencycombine"); var MongoDB = require("mongodb"); var
events = require("events"); var tools = require("tools"); var fork =
require("child_process").fork;

var currList = combine.getCurrencyCodeList(); var statsCollList =
combine.getStatsCodeList(); var statsList = combine.generateStatsList(currList);
var normCollList = combine.getNormCodeList(); const COLL_NAMES =
combine.getCollectionNames();

var mongoDbHost = tools.argv(process.argv[2],MONGO_DB_HOST); var mongoDbPort =
tools.argv(process.argv[3],MONGO_DB_PORT); var currDbName =
tools.argv(process.argv[4],CURR_DB_NAME); var doSplit =
tools.argv(process.argv[5],DO_SPLIT);

var MongoClient = MongoDB.MongoClient; var Server = MongoDB.Server;

var mongoClient = new MongoClient(new Server(mongoDbHost,mongoDbPort));

console.log("DB Host: "+mongoDbHost+"\tCurr Db: "+currDbName+"\tDo Split:"+doSplit);

var ee =new events.EventEmitter();

function Drop(_db) {   this.db = _db;   events.EventEmitter.call(this);
  this.dropAllCurrencies = function (currencyList) {     var count = 0;     var
    self = this;

    for(var i=0;i<currencyList.length;i++) {

      self.db.dropCollection(currencyList[i],function(err,result) {
        hLogErr("DROP",err);

        count++; if (count == currencyList.length) {   console.log("Dropped all");   self.emit('dropped-all',count);

      }       });     }

    }; }; Drop.prototype = Object.create(events.EventEmitter.prototype);

    /*
    /* * Error Handler */ function hErr(err) {   if (err) {

    throw err;   }

  }

  function hLogErr(category,err) {   if (err) {
    console.log(category+"\n"+err);   } }

    function rawProcessor(err,raw) {   if (doSplit=="true") {

      console.log("Processing started.."); hErr(err); var rawDrop = new
      Drop(currDb); rawDrop.on('dropped-all', function() {

        hLogErr("RAW",err); console.log("Split Collection dropped.");
        ee.on('split-next', function(tracker) {
          writeSplit(raw,currList[tracker],tracker); }); ee.emit('split-next',0);

        }).dropAllCurrencies(currList);

      } else {   console.log("Skipping split, doing normalisation and aggregate.");
      var stream = currDb.collection(COLL_NAMES.stats).find().stream();

      stream.on('data',function(data) {

        var from = data._id.split("_")[1]; statsList[from] = {}; for(var to in
          data) {   if (to!="_id" && to!=from) {     statsList[from][to] = data[to];   } }

        }).on('end', function(){   console.log("Start Normalising and Aggregation.");   normaliseAndAggregate(); });

      }

    }

    function writeSplit(raw,from,tracker) {

      console.log(from); var stream = raw.find().each(

        function(err,item) {

          if(err) {   hLogErr('RAW',err); }

          if(item == null)
          {   console.log("End Raw Table processing for",from);   tracker++;

            if(tracker==currList.length) {
              writeStats();
            } else {
                ee.emit('split-next',tracker);
            }
          }
          else
          { var rates =
              item["rates"];

                 var doc = {};   doc._id = item._id;
                 var allNaN = true;
              for(var to in rates) {
                if (from!=to) {
                  var value = rates[from]/rates[to];
                  if(!isNaN(value))
                  {
                    allNaN = false;
                    doc[to] = value;
                    updateStats(from,to,value);
                  }

              }
            }

              if(!allNaN)
              {
                  currDb.collection(from).insert(doc,{safe:true}, function(err,result) {
                  hLogErr(err);  });
              }

            }
          });
            }



            function updateStats(from,to,value) {   if(statsList[from] == null)   {
              statsList[from] = {};   }   var statsDoc = statsList[from][to];   if(statsDoc ==
                null)   {     statsDoc = {};     statsDoc.count = 0;     statsDoc.sum = 0;
                statsDoc.avg = 0;     statsDoc.max = 0;     statsDoc.min = null;   }

                statsDoc.count = statsDoc.count+1; statsDoc.sum = statsDoc.sum+value;
                statsDoc.avg = statsDoc.sum*1/statsDoc.count; if (value > statsDoc.max) {
                  statsDoc.max = value; } if (value < statsDoc.min){   statsDoc.min = value; }

                  statsDoc.range = statsDoc.max - statsDoc.min; statsList[from][to] =
                  statsDoc;   }

                  function writeStat(completed, key, statsDoc) {
                    currDb.collection(COLL_NAMES.stats).insert(statsDoc,{safe:true},function(err,result)   {
                      hLogErr("STATS",err);     completed.push(1);;

                      if (completed.length == currList.length) {   console.log("Stats done...");
                      normaliseAndAggregate(); }

                    });   }   function writeStats()   { console.log("Starting Stats Writing..."); var statsDrop = new Drop(currDb); statsDrop.on('dropped-all',
                    function() {   console.log("Dropped stats...");   var completed = [];
                    for(var key in statsList)   {  var statsDoc = statsList[key];
                      statsDoc._id = "STATS_"+key;

                      writeStat(completed, key,statsDoc);       }
                    }).dropAllCurrencies([COLL_NAMES.stats]);

                  }

                  function normaliseAndAggregate() {

                    var batchSize = currList.length*1/AGG_WORKER_COUNT; var activeTokens=[]; var
                    list = []; var batchCount = 0;

                    for(var i=0;i<currList.length;i++) {   list.push(currList[i]);   if
                      (list.length>=batchSize) {     batchCount++;
                        activeTokens.push(batchCount);

                        var cp =
                        fork("./normaliseAndAggregate",[JSON.stringify(list),MONGO_DB_HOST,MONGO_DB_PORT,CURR_DB_NAME,batchCount]);

                        cp.send(statsList);

                        cp.on('exit', function() {   activeTokens.pop();   if
                          (activeTokens.length == 0) {     console.log("Finished","\nTotal Time taken (min): ",((new Date()).getTime()-START_TIME)/60000);
                          process.exit();   } }); list = [];       }

                        }

                        if (list.length> 0) {   batchCount++;   console.log(batchCount,list);
                          activeTokens.push(batchCount);

                          var cp =
                          fork("./normaliseAndAggregate",[JSON.stringify(list),MONGO_DB_HOST,MONGO_DB_PORT,CURR_DB_NAME,batchCount]);

                          cp.send(statsList);

                          cp.on('exit', function() {   activeTokens.pop();   if (activeTokens.length ==
                            0) {     console.log("Finished","\nTotal Time taken (min): ",((new
                              Date()).getTime()-START_TIME)/60000);

                              process.exit();         }       });     }

                            } var currDb = null;

                            console.log((new Date())+"  Bulk Resync started.. obtaining database links");

                            mongoClient.open(function(err,client) {

                              hErr(err); currDb = client.db(currDbName);

                              console.log("Links obtained.");

                              currDb.collection(COLL_NAMES.raw,rawProcessor);   });
