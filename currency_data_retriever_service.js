/**
Pull data from currency portal and inject it into MongoDB
Service: (openexchangerates.org)
*/

const MONGO_COLLECTION_NAME="Raw";
const MONGO_DB_URL="mongodb://localhost:27017/Currency_v2";

const REQUEST_OPTIONS =
      {
        host: 'openexchangerates.org',  //localhost for test server
        path: '/api/latest.json?app_id=aec9d794c7104648b9264945b7a7f32c',
        //port: '18080', //for test server
        method: 'GET'
      };
const REQUEST_FREQ_IN_MINS=61;//60*1000;


const KEY_DISCLAIMER="disclaimer";
const KEY_LICENSE="license";

var http=require('http');

var mongoClient=require('mongodb').MongoClient;

var badResponseCount=0;

function loadCurrencyData()
{
  var date = new Date();
  var day = date.getDay();
  if ((day == 0 || day == 6) && process.argv[2]!="test") {
    console.log("No need to get data for Saturday or Sunday, date: "+date);
    return;
  }
  console.log("Retrieving.. "+date);
  try{
    http.request(REQUEST_OPTIONS, function(response)
                 {
                   try
                     {

                   var data='';
                   response.on('data', function(chunk)
                              {
                                data+=chunk;
                              });
                   response.on('end',function()
                              {
                                try
                                {
                                  var jsonData=JSON.parse(data);

                                  jsonData._id=new Date().getTime();

                                  delete jsonData[KEY_LICENSE];
                                  delete jsonData[KEY_DISCLAIMER];

                                  collection.insert(jsonData,{safe:true},function(err,result)
                                                 {

                                                   if(err) throw err;

                                                   console.log("Done.");
                                                   
                                                 });
                                }
                                catch(e)
                                {
                                  var time = new Date();
                                  badResponseCount++;
                                  console.log("Bad response: "+e+"\n"+time+"  count:"+badResponseCount);
                                }


                              });
                    response.on('clientError', function(error,socket)
                                {
                                  console.log("Client error: "+error);
                                });
                     }catch(e)
                       {
                         console.log(e);
                       }

                 }).on('error', function(err)
                       {
                        console.log(err);
                       }).end();
  }
  catch(e)
  {
    console.log(e);
  }
}

var collection;
mongoClient.connect(MONGO_DB_URL,function(err,db)
                    {
                      if(err) throw err;

                      db.collection(MONGO_COLLECTION_NAME, function(err,_collection)
                                   {
                                     if(err) throw err;

                                     collection=_collection;
                                     loadCurrencyData();
                                     setInterval(loadCurrencyData,REQUEST_FREQ_IN_MINS*60*1000);

                                   });




                    }
                   );








