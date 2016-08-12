using Newtonsoft.Json.Linq;
using System;
using System.Data;
using System.IO;
using System.Net;
using System.Text;
using System.Xml;
using System.Collections.Generic;

public class TaskData
{
    public Channel m_channel;       //渠道
    public Svr m_svr;           //区
    public string m_postSvrInfo;
    public  Dictionary<string, Order> m_orderDatas = new Dictionary<string, Order>();
}
//区
public class Task
{
    /// <summary>
    /// Post
    /// </summary>
    /// <param name="resultMsg"></param>
    /// <returns>ResultCode为1代表成功，0代表失败；ResultMsg：请求成功则为Success，请求失败则为异常信息内容；</returns>

    public int Post()
    {
        string posUrl = m_baseURL + m_actionNameURL;
        string strResult;
        ////test
        int resultCode = 1;
        try
        {
            if (PostWebRequest(posUrl, m_postData, m_defaultEncode, out strResult) == false)
            {
                return 0;
            }
            if (Setting.Debug())
            {
                return 1;
            }
            JObject obj = JObject.Parse(strResult);
            resultCode = (int)obj["ResultCode"];
            string resultMsg = (string)obj["ResultMsg"];
        }
        catch (Exception ex)
        {
            Log.LogError("Post Failed" + ex.ToString());
            resultCode = 0;
        }
        return resultCode;
    }



    #region 内部方法
    protected bool PostWebRequest(string postUrl, string paramData, Encoding dataEncode, out string jsonRet)
    {
        //string ret = string.Empty;
        try
        {
            byte[] byteArray = dataEncode.GetBytes(paramData); //传值参数转化byte数组
            if (Setting.Debug())
            {
                Log.LogDebug("post" + paramData);
                jsonRet = "test";
                return true;
            }
            Log.LogDebug("post:" + paramData);
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(new Uri(postUrl)); //创
            request.Method = "POST"; //请求方式
            request.ContentType = "application/x-www-form-urlencoded";//设置内容类型
            request.ContentLength = byteArray.Length; //设置请求长度
            Stream newStream = request.GetRequestStream();
            newStream.Write(byteArray, 0, byteArray.Length);//写入参数
            newStream.Close();
            //响应流
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            Stream s = response.GetResponseStream();
            XmlTextReader Reader = new XmlTextReader(s);
            Reader.MoveToContent();
            jsonRet = Reader.ReadInnerXml();//取出Content中的Json数据
            Log.LogDebug("receive:" + jsonRet);
            Reader.Close();
            s.Close();
        }
        catch (WebException ex)
        {
            //webPost可能会在这里超时导致抛出异常
            Log.LogError(ex.ToString());
            jsonRet = ex.ToString();
            return false;
            //HttpWebResponse res = (HttpWebResponse)ex.Response;
            //StreamReader sr = new StreamReader(res.GetResponseStream(), Encoding.UTF8);
            //string msg = sr.ReadToEnd();
            //Log.LogError("Post Failed got WebException" + msg);
            //Log.LogError("Error Call stack" + ex.StackTrace);
            //jsonRet = "";
            //return false;
        }
        catch (Exception ex)
        {
            //test
            //HttpWebResponse res = (HttpWebResponse)ex.Response;
            //StreamReader sr = new StreamReader(res.GetResponseStream(), Encoding.UTF8);
            //string strHtml = sr.ReadToEnd();
            Log.LogDebug("Post Failed other Exception" + ex.ToString());
            Log.LogError("Error Call stack" + ex.StackTrace);
            jsonRet = "";
            return false;
        }
        return true;
    }

    /// <summary>
    ///  得到某个聚集函数(count,sum)的执行结果
    ///  ##########之前写的用的，后面主要改为使用sqlmanager的setandexecute方法为了接口统一还是尽量替换成使用setandexecute来获取sqlCommand结果吧。
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="cmd">sql 指令</param>
    /// <param name="resultName">返回名称</param>
    /// <param name="result">返回值 输出参数</param>
    /// <param name="parmArray">可变参数列表</param>
    /// <returns>失败 写入logerror日志</returns>
    public static bool GetAggregateResult<T>(SqlCommand cmd, string resultName, ref T result, params MySql.Data.MySqlClient.MySqlParameter[] parmArray)
    {
        SqlStament st = SqlManager.GetInstance().GetSqlStament(cmd);
        try
        {
            if (st == null)
            {
                Log.LogError("sql:" + st.GetCommand() + "not register");
                return false;
            }
            foreach (MySql.Data.MySqlClient.MySqlParameter par in parmArray)
            {
                st.SetParameter(par);
            }
            DataTable table = new DataTable();
            string msg = "";
            if (!st.Execute(ref table, ref msg))
            {
                Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                return false;
            }
            else
            {
                var ret = table.Rows[0][resultName];
                // Log.LogDebug("ret type:" + ret.GetType().ToString());
                result = (T)table.Rows[0][resultName];
            }
        }
        catch (Exception ex)
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + ex.ToString());
            return false;
        }
        return true;
    }




    protected string m_postData;
    protected string m_actionNameURL;
    public string GetActionName()
    {
        return m_actionNameURL;
    }
    protected string m_baseURL = "http://mnreport.chineseall.net/ReportWs.asmx/";
    protected string m_gameName = "我的美女老师";
    protected string m_gameServerName = "";
    protected string m_userToken = "c2ab32e4t673802c";
    protected string m_gameCode = "MNLS001";
    protected Encoding m_defaultEncode = Encoding.UTF8;
    //protected string m_platform = "中文在线";
   // protected string m_postSvrInfo = "";//要提交的gamesvrip=127.0.0.+"区Id":"渠道"

   // public Channel m_channel;       //渠道
   // public Svr m_svr;           //区

    protected List<TaskData> m_taskData = new List<TaskData>();

   

    public virtual bool Init(List<Svr> svrList, List<Channel> chanList)
    {
        foreach (var svr in svrList)
        {
            foreach (var chan in chanList)
            {
                TaskData data= new TaskData();
                data.m_channel = chan;
                data.m_svr = svr;
                data.m_postSvrInfo = svr.m_SvrAreaName + ":" + chan.ChannelId;
                m_taskData.Add(data);
            }
        }
        return true;
    }


 

}
#endregion