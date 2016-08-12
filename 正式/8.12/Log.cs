using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
using System.IO;
enum FileType
    {
        Warning,
        Debug,
        Error,
    }
//日志
class Log
{
    public static bool Init()
    {
        return FileSystem.LoadLogConfig(ref m_LogFilePath);
    }

    public static void SetFileName(string strFileName)
    {
        m_LogFileName = strFileName;
    }
    private static string m_LogFileName = "";

    private static string m_LogFilePath = "D:\\";

    
   

    public static void LogDebug(string message)
    {
        FileType type = FileType.Debug;
        StackTrace st = new StackTrace(new StackFrame(1, true));
        StackFrame sf = st.GetFrame(0);
        string fileName = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-" + DateTime.Now.Day.ToString() + m_LogFileName+"." + type.ToString() ;
        string logMsg = "FileName:" + sf.GetFileName() + " MethodName:" + sf.GetMethod().Name + " LineNumber:" + sf.GetFileLineNumber() + " Debug:" + message;
        File.AppendAllText(m_LogFilePath + fileName, DateTime.Now.ToString(" HH:mm:ss    ") + logMsg + Environment.NewLine);
    }

     

    public static void LogError(string message)
    {
        FileType type = FileType.Error;
        StackTrace st = new StackTrace(new StackFrame(1, true));
        StackFrame sf = st.GetFrame(0);
        string fileName = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-" + DateTime.Now.Day.ToString() + m_LogFileName + "." + type.ToString();
        string logMsg = "FileName:" + sf.GetFileName() + " MethodName:" + sf.GetMethod().Name + " LineNumber:" + sf.GetFileLineNumber() + " Error:" + message;
        File.AppendAllText(m_LogFilePath + fileName, DateTime.Now.ToString(" HH:mm:ss    ") + logMsg + Environment.NewLine);
    }

  

    private static  void WriteFile(FileType type, string message)
    {
        StackTrace st = new StackTrace(new StackFrame(1, true));
        StackFrame sf = st.GetFrame(0);
        string fileName = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-" + DateTime.Now.Day.ToString()+"."+type.ToString();
        string logMsg = "FileName:" + sf.GetFileName() + " MethodName:" + sf.GetMethod().Name + " LineNumber:" + sf.GetFileLineNumber() + " Error:" + message;
        File.AppendAllText(m_LogFilePath + fileName, DateTime.Now.ToString(" HH:mm:ss    ") + logMsg + Environment.NewLine);
    }

}
