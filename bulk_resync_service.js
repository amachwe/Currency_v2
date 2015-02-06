var tools = require("tools");
var fork = require("child_process").fork;

const TIMER_DISABLED = 0;
const DO_SPLIT = false;

var doSplit = tools.argv(process.argv[3],DO_SPLIT);
var timerInterval = tools.argv(process.argv[2],TIMER_DISABLED)*1;
var TASK_RUNNING = false;

if (timerInterval<0) {
    console.log("Timer interval (in minutes) must be greater than or equal to zero.");
}

if (timerInterval != TIMER_DISABLED) {
    
    console.log((new Date())+ " Service running with interval (mins): "+timerInterval);
    setInterval(runTask,timerInterval*60*1000);
}
else
{
    console.log("Timer has been disabled. Nothing to do therefore exiting.");
}


function runTask() {
    
    if (TASK_RUNNING) {
        console.log((new Date())+ " Task is already running.");
        return;
    }
    
    var cp = fork("./bulk_data_resync.js",null,null,null,doSplit);
    
    TASK_RUNNING = true;
    
    cp.on('exit', function()
          {
            console.log("Resync completed.");
            TASK_RUNNING = false;
          });
}