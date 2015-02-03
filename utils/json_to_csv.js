var MongoClient = require("mongodb").MongoClient;
var Server = require("mongodb").Server;
var fs = require("fs");
var tools = require("tools");

var HOST = tools.argv(process.argv[2],"localhost");
var PORT = tools.argv(process.argv[3],27017);
var DB = tools.argv(process.argv[4],"Currency_v2");
var COLL =  tools.argv(process.argv[5],"TARGET");
var OUTPUT = tools.argv(process.argv[6],"TARGET.CSV");



var mongoClient = new MongoClient(new Server(HOST,PORT));


mongoClient.open(function(err,client)
                 {
                    var db = client.db(DB);
                    var stream = db.collection(COLL).find().stream();
                    var first = true;
                   
                    var ws = fs.createWriteStream(OUTPUT,{encoding: "UTF-8"});
                    console.log("Starting writing: ",COLL,OUTPUT);
                    stream.on('data', function(data){
                            if (first) {
                                first = false;
                                for(var key in data)
                                {
                                    ws.write(key+",");
                                }
                                
                                ws.write("\n");
                                
                            }
                            
                            
                            
                                for(var key in data)
                                {
                                        console.log(key+" "+data[key]);
                                        ws.write(data[key]+",");
                                }
                                    
                                ws.write("\n");
                            
                            
                            
                        });
                 });