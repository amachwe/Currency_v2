var tools = require("tools");
var Server = require("ws").Server;
var cluster = require("cluster");

const PORT = process.env.PORT || 4000;
const WORKER_COUNT = 4;

var server = new Server({port:PORT});

if (cluster.isMaster) {
    console.log("Splitter service: awaiting connection on "+PORT);
    for(var i = 0;i<WORKER_COUNT;i++)
    {
        cluster.fork();
    }
}

var Socket = null;
if (cluster.isWorker) {
    
    server.on('connection', hConnection);
}

function hConnection(socket)
{
    console.log("Incoming connection.."+socket);
    Socket = socket;
    Socket.on('message', hMessage);
}

function hMessage(message)
{
    console.log(message);
}