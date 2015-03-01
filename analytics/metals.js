var MongoClient = require("mongodb").MongoClient;
var Server = require("mongodb").Server;
var tools = require("tools");
var combine = require("currencycombine");
var ee = require("events").EventEmitter;

var currList = combine.getNormCodeList();

var HOST = tools.argv(process.argv[2],"localhost");
var PORT = tools.argv(process.argv[3],27017);
var DB = tools.argv(process.argv[4],"Currency_v2");


var mongoClient = new MongoClient(new Server(HOST,PORT));

var event = new ee();

var STAGE1 = [
              
              {

                $project : {
                            _id :0,
                            ts: "$_id",
                            XAU:1,
                            XAG:1,
                            avg : {}
                        }
                    }
                ];

var STAGE2 = [
              {
                $group : {
                            _id : "$ts",
                            avAg :
                                {
                                    $avg : "$XAG"
                                },
                            avAu :
                                {
                                    $avg : "$XAU"
                                },
                            av :
                                {
                                    $push : "$avg"
                                }

                            }
                        }
                    ];

var TGT_STG1_COLL_NAME = "AN_METALS";
var TGT_STG2_COLL_NAME = "AGG_AN_METALS";

mongoClient.open(function(err,client)
                 {
                    tools.err(err);
                    console.log("Start processing...");

                    var currDb = client.db(DB);

                    var stg1Coll = currDb.collection(TGT_STG1_COLL_NAME);
                    var stg2Coll = currDb.collection(TGT_STG2_COLL_NAME);
                    var count = 0;

                    console.log("Got target collection");
                    stg1Coll.drop(function(err,res)
                                 {
                                    STAGE1[0].$project.avg.base = {$literal : currList[count].split("_")[1]};
                                    STAGE1[0].$project.avg.val = "$_avg";
                                    executeStg(currDb,stg1Coll,currList[count],STAGE1,'stg1done');

                                    event.on('stg1done',function(curr)
                                    {
                                        count++;
                                        console.log("Done",curr);

                                      var nextCurr = currList[count];
                                      if (nextCurr==null) {
                                        console.log("Stage 1 Done, starting Stage 2");

                                        stg2Coll.drop(function(err,res)
                                                      {
                                                        event.on('stg2done', function(curr)
                                                        {
                                                            console.log(curr,"done");
                                                            process.exit();
                                                        });
                                                        executeStg(currDb,stg2Coll,TGT_STG1_COLL_NAME,STAGE2,'stg2done');

                                                      });
                                      }
                                      else
                                      {

                                        STAGE1[0].$project.avg.base = {$literal : currList[count].split("_")[1] };
                                        STAGE1[0].$project.avg.val = "$_avg";
                                        executeStg(currDb,stg1Coll,nextCurr,STAGE1,'stg1done');
                                      }
                                    });

                                 });






                 });


function executeStg(currDb, tgtColl,curr,query,msgId){

            currDb.collection(curr, function(err,coll)
                                               {
                                                tools.err(err);
                                                var cursor = coll.aggregate(query,{cursor: {batchSize:1000}});
                                                var count = 0;
                                                cursor.each(function(err,data)
                                                            {
                                                                if (data!=null) {

                                                                    tgtColl.insert(data,{safe:true}, function(err,result)
                                                                                   {if (err) throw err;});


                                                                }
                                                                else
                                                                {
                                                                    event.emit(msgId,curr);
                                                                }

                                                            });
                                               });


}
