var tools = require("tools");
var fork = require("child_process").fork;

const TIMER_DISABLED = 0;


var timeInterval = tools.argv(process.argv[2],TIMER_DISABLED)*1;
var TASK_RUNNING = false;

if (timerInterval<0) {
    console.log("Timer interval (in minutes) must be greater than or equal to zero.");
}

if (timeInterval != TIMER_DISABLED) {
    setInterval(runTask,timerInterval*60*1000);
}
else
{
    console.log("TIMER has been disabled. Nothing to do and exiting.");
}


function runTask() {
    
    if (TASK_RUNNING) {
        console.log("Task is running already.");
        return;
    }
    
    var cp = fork("./bulk_data_resync.js");
    
    TASK_RUNNING = true;
    
    cp.on('exit', function()
          {
            console.log("Resync completed.");
            TASK_RUNNING = false;
          });
}