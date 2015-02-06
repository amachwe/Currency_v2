

module.exports = new function()
{

  
    this.argv = function (arg,defVal)
    {
        if (arg!=null && arg!="null") {
            
            return arg;
        }
        else
        {
            return defVal;
        }
    };
  
    
    
    this.err = function(err)
    {
      if(err)
      {
        throw err;
      }
    };
    
    this.logErr = function(err)
    {
      if(err)
      {
        console.error(err);
      }
    };
}

