
var currList = JSON.parse(process.argv[2]);
var host = process.argv[3];
var port = process.argv[4];
var dbName = process.argv[5];
var batchCount = process.argv[6];

var START_TIME = (new Date()).getTime();

var events = require("events");
var MongoClient = require("mongodb").MongoClient;
var Server = require("mongodb").Server;
var combine = require("currencycombine");

var mongoClient = new MongoClient(new Server(host,port));

var statsList = null;
var normCollList = [];

for(var i =0; i<currList.length; i++)
{
    normCollList.push("NORM_"+currList[i]);
}
const COLL_NAMES = combine.getCollectionNames();
process.on('message', function(msg)
{
    statsList = msg;

    console.log(batchCount, "Executing with",currList.length,"currencies");
    mongoClient.open(function(err,client){
        if (err) {
            throw err;
        }
        var currDb = client.db(dbName);
        (new Drop(currDb)).on('dropped-all',function()
                              {

        var totalDocs = statsList["USD"]["GBP"].count*currList.length;

        var stream = currDb.collection(COLL_NAMES.raw).find().stream();



        var itemCount = 0;
        stream.on('data',
                function(item)
                {

                    var rates = item["rates"];

                    for (var i=0;i<currList.length;i++) {
                            var from = currList[i];
                            var collName = "NORM_"+from;
                            var normDoc = {};
                            normDoc._id = item._id;

                            var statsDoc = statsList[from];
                            var sum = 0;
                            var avg = 0;
                            var count = 0;

                            for(var to in rates)
                            {

                             if (to!="_id" && from!=to) {
                               

                               if (statsDoc[to].max != statsDoc[to].min) {

                                    var value = rates[from]/rates[to];
                                    normDoc[to]=(value-statsDoc[to].min)/(statsDoc[to].max-statsDoc[to].min);
                               }
                               else
                               {
                                    normDoc[to]=statsDoc[to].max;
                               }

                               sum+=normDoc[to]*1;
                               count+=1;

                             }

                            }

                            avg = sum*1/count;

                            var sqrSum = 0;

                            for(var to in normDoc)
                            {
                                if (to!="_id") {


                                     sqrSum+=(normDoc[to]-avg)*(normDoc[to]-avg)*1;
                                }

                            }

                            normDoc["_count"] = count;
                            normDoc["_sum"] = sum;
                            normDoc["_avg"] = avg;
                            normDoc["_stdev"] = Math.sqrt(sqrSum/count);
                            normDoc["_sqrSum"] = sqrSum;



                            //console.log(">>",itemCount++);

                            currDb.collection(collName).insert(normDoc,{safe:true}, function(err,result)
                                   {


                                        hErr(err);
                                        //console.log("<<",totalDocs);
                                        totalDocs--;

                                        if (totalDocs == 0) {

                                          currDb.close();

                                          console.log("All done...");
                                          console.log(batchCount,"Finished","\nTotal Time taken (min): ",((new Date()).getTime()-START_TIME)/60000);
                                          process.exit();
                                        }

                                        if (totalDocs < 0) {
                                          console.log("Count in statistics less than data count, possible data corruption. Please resync from Raw data again");
                                        }

                                   });
                    }


                }).on('end',function ()
                    {

                        console.log("End Agg/Normal Raw Table processing.");


                    });
    }).dropAllCurrencies(normCollList);});

} );

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
                                                         hLogErr("DROP",err);

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
