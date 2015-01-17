var tools = require("tools");
var Server = require("ws").Server;
var cluster = require("cluster");

const PORT = process.env.PORT || 8000;
const WORKER_COUNT = 4;

var id = 0;


if (cluster.isMaster) {
    console.log("Splitter service: awaiting connection on "+PORT);
    for(var i = 0;i<WORKER_COUNT;i++)
    {
        cluster.fork();
    }
}


if (cluster.isWorker) {
    
    var server = new Server({port:PORT});
  
    var workerId = ++id;
    
    var Socket = null;
    try
    {
        server.on('connection', 
            function (socket)
            {
                console.log(workerId+" Incoming connection.."+socket);
                Socket = socket;
                Socket.on('message',
                    function (message)
                    {
                        console.log(message);
                    });
            });
    }catch(e)
    {
        console.log("Error opening socket connection > "+e);
    }
}


