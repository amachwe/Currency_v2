var express = require('express');
var combine = require('currencycombine');


var app = express();

const PORT=process.env.PORT || 3999;
/*
Help
*/
app.get("/", function(request,response)
       {
         response.header("Access-Control-Allow-Origin", "*");
         response.header("Access-Control-Allow-Headers", "X-Requested-With");

         response.sendFile(
           'help.htm', {root: __dirname}
         );
       });
/*
Full currency list
*/
app.get("/currency/list", function(request,response)
       {
         response.header("Access-Control-Allow-Origin", "*");
         response.header("Access-Control-Allow-Headers", "X-Requested-With");

         response.send(combine.getCurrencyList());
       });

/*
Sequence Stream
*/
app.get("/currency/sequence/:code", function(request,response)
       {
         response.header("Access-Control-Allow-Origin", "*");
         response.header("Access-Control-Allow-Headers", "X-Requested-With");

         getCurrencyStream(request.params.code, response);

       });

/*
Sequence Normalised Stream
*/
app.get("/currency/sequence/normalised/:code", function(request,response)
       {
         response.header("Access-Control-Allow-Origin", "*");
         response.header("Access-Control-Allow-Headers", "X-Requested-With");

         getCurrencyStream(request.params.code, response,true);

       });

/*
Sequence Aggregate Stream
*/
app.get("/currency/sequence/aggregate/:code", function(request,response)
       {
         response.header("Access-Control-Allow-Origin", "*");
         response.header("Access-Control-Allow-Headers", "X-Requested-With");

         getCurrencyStream(request.params.code, response,true);

       });

/*
Full Average Sequence vs Code
*/
app.get("/currency/sequence/analysis/:type", function(request,response)
        {
          response.header("Access-Control-Allow-Origin", "*");
          response.header("Access-Control-Allow-Headers", "X-Requested-With");

          getAnalysisStream(request.params.type,response);
        });

/*
Date Range Query
*/
app.get("/currency/sequence/range/date/:startDate/:endDate/:code", function(request,response)
        {
          response.header("Access-Control-Allow-Origin", "*");
          response.header("Access-Control-Allow-Headers", "X-Requested-With");

          getRangeDateStream(request.params.startDate,request.params.endDate,request.params.code,response);
        });

app.listen(PORT);
console.log("Currency API Active on port: "+PORT);

/*
Implementation
*/
var mongoClient= require('mongodb').MongoClient;
var Stream = require('stream');
var JSONStream = require('JSONStream');

const currency_list = combine.getCurrencyList();
const MONGO_DB_URL="mongodb://localhost:27017/Currency_v2";


function getCurrencyStream(code,response,normalised)
{

      if(currency_list[code]!=null)
       {
         mongoClient.connect(MONGO_DB_URL, function(err,db)
                       {
                         if(err) throw err;

                              if (normalised!=null && normalised == true) {
                                code = "NORM_"+code;
                              }
                             db.collection(code, function(err,coll)
                                      {
                                        if(err) throw err;
                                        response.set('Content-Type', 'application/json');

                                        var stream = coll.find().stream();
                                        stream.on('end', function()
                                                  {
                                                    db.close();
                                                  })
                                        stream.pipe(JSONStream.stringify()).pipe(response);

                                      });


                       });
        }
       else
        {

           response.send("Currency code not found: "+code+"");

        }
}

function getAnalysisStream(type,response)
{
    if (type == null) {
      response.send("Bad analytics type.");
      response.end();
      return;
    }
    var collName = "";
    if (type == "metals") {
      collName = "AGG_AN_METALS";
    }

    if (collName == "") {
      response.send("Empty analytics type.");
      response.end();
      return;
    }
    mongoClient.connect(MONGO_DB_URL, function(err,db)
                       {
                         if(err) throw err;


                             db.collection(collName, function(err,coll)
                                      {
                                        if(err) throw err;
                                        response.set('Content-Type', 'application/json');

                                        var stream = coll.find().stream();
                                        stream.on('end', function()
                                                  {
                                                    db.close();
                                                  })
                                        stream.pipe(JSONStream.stringify()).pipe(response);

                                      });


                       });
}

function getRangeDateStream(startDate,endDate,code,response)
{
  var startTs = new Date(startDate).getTime();
  var endTs = new Date(endDate).getTime();

  response.write("Start TS: "+startTs+"   End TS: "+endTs+"  Code: "+code);

  var count = 0;
  mongoClient.connect(MONGO_DB_URL,function(err,db)
  {
    if(err) throw err;

    db.collection(code,function(err,coll)
    {
      if(err) throw err;
      var stream = coll.find({ $and : [
        {_id :
          {$gte :
             startTs
          }
        }, {
          _id :
          {$lte :
             endTs

          }
        }]}).stream();
      stream.on('data', function(item)
      {
        if(item._id >= endTs)
        {
          db.close();
          steam = null;
          response.write(" Result size: "+count);
          response.end();
          return;
        }
        count++;
        response.write("\n"+count+ " > "+item._id);

      }).on('end',function()
      {
        db.close();
        response.write(" Result size: "+count);
        response.end();

      });
    });
  });
}
