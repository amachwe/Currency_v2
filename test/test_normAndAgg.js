var fork = require("child_process").fork;
var statsList = require("./stats_test.json");
var START_TIME = (new Date()).getTime();
 var cp = fork("./normaliseAndAggregate",[JSON.stringify(["USD","GBP","AED"]),"localhost",27017,"Currency_v2"]);
            cp.send(statsList);
         
            
            