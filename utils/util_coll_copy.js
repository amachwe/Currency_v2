var MongoClient = require("mongodb").MongoClient;

const SOURCE = "mongodb://192.168.0.29:27017/Currency_v2";
const TARGET = "mongodb://localhost:27017/Currency_v2";
const SRC_COLL = "Raw";
const TGT_COLL = "Raw";


/*
 *Helper: check process argv and put in variable
 */

function argv(arg,defVal)
{
    if (arg!=null) {
        
        return arg;
    }
    else
    {
        return defVal;
    }
}

var dropTarget = argv(process.argv[2]);



MongoClient.connect(SOURCE,function(err,srcDb)
                    {
                       if(err) throw err;
                       MongoClient.connect(TARGET, function(err,tgtDb)
                                           {
                                                if (err) throw err;
                                                
                                                srcDb.collection(SRC_COLL,function(err,coSrc)
                                                                 {
                                                                    if (err) throw err;
                                                                    if (dropTarget=="drop") {
                                                                     
                                                                    }
                                                                    tgtDb.collection(TGT_COLL,function(err,coTgt)
                                                                                     {
                                                                                       if (err) throw err;
                                                                                       if(dropTarget == "true")
                                                                                       {
                                                                                          coTgt.drop(function(err,result)
                                                                                                     {
                                                                                                      
                                                                                                      var count = 0;
                                                                                                      if (err) throw err;
                                                                                                      console.log("Table dropped.");
                                                                                                      var stream = coSrc.find().stream();
                                                                                                      stream.on('data', function(item)
                                                                                                                {
                                                                                                                  coTgt.insert(item,{safe:true}, function(err,result)
                                                                                                                               {
                                                                                                                                  count++;
                                                                                                                                  if (err) throw err;
                                                                                                                               });
                                                                                                                }).on('end',function()
                                                                                                                      {
                                                                                                                          
                                                                                                                      });
                                                                                                     });
                                                                                       }
                                                                                       else
                                                                                       {
                                                                                           var count = 0;
                                                                                                      if (err) throw err;
                                                                                                      var stream = coSrc.find().stream();
                                                                                                      stream.on('data', function(item)
                                                                                                                {
                                                                                                                  coTgt.insert(item,{safe:true}, function(err,result)
                                                                                                                               {
                                                                                                                                  count++;
                                                                                                                                  if (err) throw err;
                                                                                                                               });
                                                                                                                }).on('end',function()
                                                                                                                      {
                                                                                                                          
                                                                                                                      });
                                                                                       }
                                                                                     });
                                                                 });
                                           });
                    });


