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
Specific Sequence Stream
*/
app.get("/currency/sequence/specific/:code/:to", function(request,response)
        {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Headers", "X-Requested-With");

            getCurrencyStreamSpecific(request.params.code,request.params.to, response);

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
app.get("/currency/sequence/:code/range/:startDate/:endDate/", function(request,response)
        {
          response.header("Access-Control-Allow-Origin", "*");
          response.header("Access-Control-Allow-Headers", "X-Requested-With");

          getRangeDateStream(request.params.startDate,request.params.endDate,request.params.code,response);
        });


/*
Specific Range Sequence Stream
*/
app.get("/currency/sequence/specific/:code/:to/range/:startDate/:endDate", function(request,response)
        {
          response.header("Access-Control-Allow-Origin", "*");
          response.header("Access-Control-Allow-Headers", "X-Requested-With");

          getSpecificRangeDateStream(request.params.startDate,request.params.endDate,request.params.code,request.params.to,response);

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

function getSpecificRangeDateStream(startDate,endDate,code,to,response)
{
  if(currency_list[code]!=null)
   {
     mongoClient.connect(MONGO_DB_URL, function(err,db)
                   {
                     if(err) throw err;


                         db.collection(code, function(err,coll)
                                  {


                                    if(err) throw err;

                                    response.write("[[\"TimeStamp\",\""+to+"\"]");
                                    coll.find({ $and : [
                                      {_id :
                                        {$gte :
                                           startDate*1
                                        }
                                      }, {
                                        _id :
                                        {$lte :
                                           endDate*1

                                        }
                                      }]}).each(
                                      function(err,item)
                                      {
                                        if(err)
                                        {
                                          response.write(err);
                                          response.end();
                                        }
                                        if(item==null)
                                        {
                                          response.write("]");
                                          response.end();
                                        }
                                        if(item!=null)
                                        {
                                          var value = 0;
                                          var id = item["_id"];
                                          for(var key in item)
                                          {
                                              if(key==to)
                                              {
                                                value = item[key];
                                                break;
                                              }
                                          }
                                          response.write(",")
                                          response.write(JSON.stringify([id,value]));

                                        }

                                      }
                                    );


                                  });


                   });
    }
   else
    {

       response.send("Currency code not found: "+code+"");
       response.end();

    }
}

function getCurrencyStreamSpecific(code,to,response)
{

  if(currency_list[code]!=null)
   {
     mongoClient.connect(MONGO_DB_URL, function(err,db)
                   {
                     if(err) throw err;


                         db.collection(code, function(err,coll)
                                  {


                                    if(err) throw err;

                                    response.write("[[\"TimeStamp\",\""+to+"\"]");
                                    coll.find().each(
                                      function(err,item)
                                      {
                                        if(err)
                                        {
                                          response.write(err);
                                          response.end();
                                        }
                                        if(item==null)
                                        {
                                          response.write("]");
                                          response.end();
                                        }
                                        if(item!=null)
                                        {
                                          var value = 0;
                                          var id = item["_id"];
                                          for(var key in item)
                                          {
                                              if(key==to)
                                              {
                                                value = item[key];
                                                break;
                                              }
                                          }
                                          response.write(",")
                                          response.write(JSON.stringify([id,value]));

                                        }

                                      }
                                    );


                                  });


                   });
    }
   else
    {

       response.send("Currency code not found: "+code+"");
       response.end();

    }
}
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
           response.end();

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
  var startTs = startDate*1;
  var endTs = endDate*1;

  var data = [];
  data.push({StartTS:startTs, EndTS: endTs,  Code: code});

  var count = 0;
  mongoClient.connect(MONGO_DB_URL,function(err,db)
  {
    if(err) throw err;

    db.collection(code,function(err,coll)
    {
      if(err) throw err;

      coll.find({ $and : [
        {_id :
          {$gte :
             startTs
          }
        }, {
          _id :
          {$lte :
             endTs

          }
        }]}).each(function(err,item)
      {

        if(err) throw err;
        if(item == null)
        {
          db.close();
          data.push({Result_size:count});
          response.write(JSON.stringify(data));
          response.end();
          return;
        }
        if(item._id >= endTs)
        {
          db.close();
          steam = null;
          data.push({Result_size:count});
          response.write(JSON.stringify(data));
          response.end();
          return;
        }
        count++;
        data.push(item);

      });
    });
  });
}
