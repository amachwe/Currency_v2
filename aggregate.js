var MongoClient = require("mongodb").MongoClient;
var Server =  require('mongodb').Server;


var MONGO_DB_URL = "mongodb://localhost:27017";
var DB = "Currency";
var AGG_DB = "CurrencyAggregate";

var db_url = MONGO_DB_URL+"/"+DB;
var agg_db_url = MONGO_DB_URL+"/"+AGG_DB;
var base = null;
var normal = false;
var token = -1;
var docId = 0;
//DOC ID (int)
if (process.argv[7] !=null) {
    docId = process.argv[7]*1
}
//TOKEN (int)
if (process.argv[6] !=null) {
    token = process.argv[6];
}
//TARGET DB (url)
if (process.argv[5] !=null) {
    agg_db_url = process.argv[5];
}

//SOURCE DB (url)
if (process.argv[4] !=null) {
    db_url = process.argv[4];
}

//USE NORMALISED DATA (bool)
if (process.argv[3] !=null) {
    if (process.argv[3]=="true") {
        normal = true;
    }  
}

//BASE CURRENCY LIST (string)
if (process.argv[2] !=null) {
    
    baseList = JSON.parse(process.argv[2]);
}



console.log(token+ " Db Url: "+db_url);

console.log(token+" Agg Db Url: "+agg_db_url);

console.log(token+" Base: "+baseList);

console.log(token+" Normalised: "+normal);



var sourceDb = "Currency_v2";
var targetDb = "CurrencyAggregate_v2";
var host = "localhost";
var port = 27017;

console.log("Token: "+token+" Host: "+host+" Port: "+port+" Source: "+sourceDb+" Target: "+targetDb+" Doc Id: "+docId);


var mongoClient = new MongoClient(new Server(host,port));

if (baseList!=null) {
    var running = [];
    mongoClient.open(function(err, client)
                        {
                            if(err) throw err;
                            
                            var basedb = client.db(sourceDb);
                            var aggdb = client.db(targetDb);
                          
                            for (var i=0;i<baseList.length;i++) {

                                var base = baseList[i];
                                
                                if (docId == 0) {
                                    
                                    task(client,basedb,aggdb,base,running);
                                }
                                else
                                {
                                    
                                    deltaTask(client,basedb,aggdb,base,running,docId);
                                }
                            }                  
                            
                        });

}
else
{
    console.log("Usage: node aggregate.js <base code> true|false <source_url> <target_url>");
    console.log("true|false for use normalised data.");
}

/*
 * Carry out aggregation in delta mode - existing aggregation data will be preserved, only new document will be processed.
 */
function deltaTask(client, basedb, aggdb, base,running,docId)
{
       
    running.push(base);
    var aggCollName = "AGG_"+base;
    var baseColl ="";
    if (normal) {
        baseColl = "NORM_"+base;
    }
    else
    {
        baseColl = base;
    }
    
    if (basedb == null) {
        console.log("Error: "+base+" db null.");
    }
    if (docId == 0 || docId == null) {
        console.log("Error: Doc Id is null or zero for delta mode");
    }
    basedb.collection(baseColl,function(err,coll)
    {
        if (err) throw err;
         
       
        var docCount = 1;
        
       
                                    
                                    
                                
                                                                        
                                        
    aggdb.collection(aggCollName,function(err,aggColl)
    {
        if (err) throw err;
                      
        var stream = coll.find({_id:docId}).stream();
        
    
        stream.on('data', function(doc)
            {
               var agg_doc = {};
               agg_doc._id = doc._id;
               var sum = 0;
               var count = 0;
               var sqrSum = 0;
               
               for(var to in doc)
               {
                if (to!="_id") {
                    sum = sum+(doc[to]*1);
                }
                
                count +=1;
               }
                var avg = sum/count;
                
                for(var to in doc)
                {
                    if (to!="_id") {
                        
                    
                    var val = doc[to];
                    
                    var diff = (val-avg);
                    sqrSum = sqrSum + (diff*diff);
                   
                    }
                }
                
               agg_doc.stdev = Math.sqrt(sqrSum/count);
               agg_doc.sqrSum = sqrSum;
               agg_doc.avg = avg;
               agg_doc.sum = sum;
               agg_doc.count = count;
               
               aggColl.insert(agg_doc,{safe:true}, function(err,result)
                              {
                                if (err) throw err;
                                
                                docCount--;
                                if (docCount<=0) {
                                 
                                 try{
                                    
                                    
                                    running.pop();
                                    if (running.length == 0) {
                                        console.log("Closing DB connections");
                                        basedb.close();
                                        aggdb.close();
                                        console.log("Done");
                                    }
                                 }
                                 catch(e)
                                 {
                                    console.log("Error: "+e);
                                 }
                                }
                              });
               
            });
    });
                                  
        


 
  });
                
                        
}
/*
 * Carry out aggregation in full resync mode - existing aggregation data will be dropped and re-created.
 */
function task(client, basedb, aggdb, base,running)
{
       
    running.push(base);
    var aggCollName = "AGG_"+base;
    var baseColl ="";
    if (normal) {
        baseColl = "NORM_"+base;
    }
    else
    {
        baseColl = base;
    }
    
    if (basedb == null) {
        console.log("Error: "+base+" db null.");
    }
    basedb.collection(baseColl,function(err,coll)
    {
        if (err) throw err;
         
       
        var docCount = 0;
        
        coll.count(function(err,count)
                                  {
                                    if (err) throw err;
                                    docCount = count;
                                    if (docCount == 0) {
                                        console.log("Error: documents to be processed are zero.");
                                    }
                                    
                                    
                                    aggdb.collection(aggCollName).drop(function()
                                                                       {
                                                                        
                                        
                                                                        aggdb.collection(aggCollName,function(err,aggColl)
                                                                            {
                                                                                if (err) throw err;
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                var stream = coll.find().stream();
                                                                                
                                                                            
                                                                                stream.on('data', function(doc)
                                                                                    {
                                                                                       var agg_doc = {};
                                                                                       agg_doc._id = doc._id;
                                                                                       var sum = 0;
                                                                                       var count = 0;
                                                                                       var sqrSum = 0;
                                                                                       
                                                                                       for(var to in doc)
                                                                                       {
                                                                                        if (to!="_id") {
                                                                                            sum = sum+(doc[to]*1);
                                                                                        }
                                                                                        
                                                                                        count +=1;
                                                                                       }
                                                                                        var avg = sum/count;
                                                                                        
                                                                                        for(var to in doc)
                                                                                        {
                                                                                            if (to!="_id") {
                                                                                                
                                                                                            
                                                                                            var val = doc[to];
                                                                                            
                                                                                            agg_doc[to] = val;
                                                                                            
                                                                                            var diff = (val-avg);
                                                                                            sqrSum = sqrSum + (diff*diff);
                                                                                           
                                                                                            }
                                                                                        }
                                                                                        
                                                                                       agg_doc.stdev = Math.sqrt(sqrSum/count);
                                                                                       agg_doc.sqrSum = sqrSum;
                                                                                       agg_doc.avg = avg;
                                                                                       agg_doc.sum = sum;
                                                                                       agg_doc.count = count;
                                                                                       
                                                                                       aggColl.insert(agg_doc,{safe:true}, function(err,result)
                                                                                                      {
                                                                                                        if (err) throw err;
                                                                                                        
                                                                                                        docCount--;
                                                                                                        if (docCount<=0) {
                                                                                                         
                                                                                                         try{
                                                                                                            
                                                                                                            
                                                                                                            running.pop();
                                                                                                            if (running.length == 0) {
                                                                                                                console.log("Closing DB connections");
                                                                                                                basedb.close();
                                                                                                                aggdb.close();
                                                                                                                console.log("Done");
                                                                                                            }
                                                                                                         }
                                                                                                         catch(e)
                                                                                                         {
                                                                                                            console.log("Error: "+e);
                                                                                                         }
                                                                                                        }
                                                                                                      });
                                                                                       
                                                                                    });
                                                                            });
                                                                       });
                                        
                                
                                
                                    });
                                  });
                
                        
}