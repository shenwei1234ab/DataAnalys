using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
using System.Configuration;
//日志
class FileSystem
{
    public static bool LoadDBConfig(ref string netlogCon, ref string tdgameCon)
    {
        try
        {
            netlogCon = ConfigurationManager.AppSettings["netlogConn"];
             tdgameCon = ConfigurationManager.AppSettings["tdgameCon"];
           
        }
        catch (Exception ex)
        {
            Log.LogError(ex.ToString());
            return false;    
        }
        return true;
    }


    public static bool LoadLogConfig(ref string logFilePath)
    {
        try
        {
            logFilePath = ConfigurationManager.AppSettings["logFilePath"];
        }
        catch (Exception ex)
        {
            Log.LogError(ex.ToString());
            return false;
        }
        return true;
    }
}
