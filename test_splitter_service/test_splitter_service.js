var ws = require("ws");



var request =
{
    
    source: SERVER+"/"+DB,
    target: SERVER+"/"+AGG_DB,
    baseList: currencyList,//{"USD":1,"GBP":2,"INR":3,"AED":4,"CAD":5,"EUR":6,"JPY":7,"NGN":8},
    normalise: true
}





console.log("Sending request...  ");
var socket = new ws("ws://localhost:4000");
socket.on('open',function()
          {
            socket.send(JSON.stringify(request));
            socket.close();
            
          });

