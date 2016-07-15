using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Net;
using System.IO;
using Newtonsoft.Json.Linq;
using System.Xml;
using System.Web.Script.Serialization;
using Newtonsoft.Json;
public class AnalysisSystem
{
    /// <summary>
    /// Post
    /// </summary>
    /// <param name="resultMsg"></param>
    /// <returns>ResultCode为1代表成功，0代表失败；ResultMsg：请求成功则为Success，请求失败则为异常信息内容；</returns>

    public int Post(ref string resultMsg)
    {
        string posUrl = m_baseURL + m_actionNameURL;
        string strResult;
        int resultCode = 1;
        try
        {
            strResult = PostWebRequest(posUrl, m_postData, m_defaultEncode);
            if( Setting.Debug())
            {
                return 1;
            }
            JObject obj = JObject.Parse(strResult);
            resultCode = (int)obj["ResultCode"];
            resultMsg = (string)obj["ResultMsg"];   
        }
        catch (WebException ex)
        {
            HttpWebResponse res = (HttpWebResponse)ex.Response;
            StreamReader sr = new StreamReader(res.GetResponseStream(), Encoding.UTF8);
            resultMsg = sr.ReadToEnd();
            Log.LogError("Post Failed" + resultMsg);
            resultCode = 0;

        }
        return resultCode;
    }



    #region 内部方法
    protected string PostWebRequest(string postUrl, string paramData, Encoding dataEncode)
    {
        string ret = string.Empty;
        try
        {
            byte[] byteArray = dataEncode.GetBytes(paramData); //传值参数转化byte数组
        if(Setting.Debug())
        {
            Log.LogDebug("postUrl:" + postUrl + "svrid:" + m_svr.m_SvrAreaId + "channel:" + m_channel.Id + "data:" + paramData);
            return "test";

        }
            Log.LogDebug("postUrl:" + postUrl + "svrid:" + m_svr.m_SvrAreaId + "channel:" + m_channel.Id + "data:" + paramData);
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(new Uri(postUrl)); //创建HttpWebRequest实例
            request.Method = "POST"; //请求方式
            request.ContentType = "application/x-www-form-urlencoded";//设置内容类型
            request.ContentLength = byteArray.Length; //设置请求长度
            Stream newStream = request.GetRequestStream();
            newStream.Write(byteArray, 0, byteArray.Length);//写入参数
            newStream.Close();
            //响应流
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            //StreamReader sr = new StreamReader(response.GetResponseStream(), 
            //    dataEncode);
            //ret = sr.ReadToEnd();
            //Console.Write(ret);
            //sr.Close();
            //response.Close();
            //newStream.Close();
            Stream s = response.GetResponseStream();
            XmlTextReader Reader = new XmlTextReader(s);
            Reader.MoveToContent();
            ret = Reader.ReadInnerXml();//取出Content中的Json数据
            Reader.Close();
            s.Close();
        }
        catch (WebException ex)
        {
            //test
            //HttpWebResponse res = (HttpWebResponse)ex.Response;
            //StreamReader sr = new StreamReader(res.GetResponseStream(), Encoding.UTF8);
            //string strHtml = sr.ReadToEnd();
            throw ex;
        }
        return ret;
    }

    /// <summary>
    ///  得到某个聚集函数(count,sum)的执行结果
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
                Log.LogDebug("ret type:" + ret.GetType().ToString());
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




    /// <summary>
    /// 计算time日账号 流存率  days:0 当日  -1:昨日  
    /// </summary>
    /// <returns><0 出错   >0 留存率</returns>
    public  decimal GetAccountRetention(DateTime time, int days)
    {
        DateTime date = time.AddDays(days);
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
new MySql.Data.MySqlClient.MySqlParameter("?date", date.ToString()),
new MySql.Data.MySqlClient.MySqlParameter("?Channel",m_channel.Id),
new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId)};
        //计算当日新登账号数()
        Int64 newAddCount = 0;
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_NEWAccount, "count", ref newAddCount, array))
        {
            return -1;
        }
        if (newAddCount == 0)
        {
            return 0;
        }
        //计算当日新登账号在今日依然登陆的账号s
        System.Int64 stillLoginCount = 0;
        MySql.Data.MySqlClient.MySqlParameter[] pars = { 
          new MySql.Data.MySqlClient.MySqlParameter("?date", time.AddDays(days).ToString()),
           new MySql.Data.MySqlClient.MySqlParameter("?Channel",m_channel.Id),
          new MySql.Data.MySqlClient.MySqlParameter("?datelogin", time.ToString()),
          new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)
                                                        };
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_STILL_LOGINACCOUNT, "count", ref stillLoginCount, pars))
        {
            return -1;
        }
        return (decimal)stillLoginCount / newAddCount;
    }



    /// <summary>
    /// 计算time日角色流存率  days:0 当日  -1:昨日  
    /// </summary>
    /// <returns></returns>
    public  decimal GetPlayerRetention(DateTime time, int days)
    {
        DateTime date = time.AddDays(days);
        MySql.Data.MySqlClient.MySqlParameter[] array = 
        { 
            new MySql.Data.MySqlClient.MySqlParameter("?date", date.ToString()),
            new MySql.Data.MySqlClient.MySqlParameter("?Channel",m_channel.Id),
           new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId)
        };
        //计算当日新增角色
        Int64 newAddCount = 0;
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_NEWPLAYER, "count", ref newAddCount, array))
        {
            return -1;
        }
        if (newAddCount == 0)
        {
            return 0;
        }
        //计算当日注册账号在第days日依然登陆的角色
        System.Int64 stillLoginCount = 0;

        MySql.Data.MySqlClient.MySqlParameter[] pars = { 
          new MySql.Data.MySqlClient.MySqlParameter("?date", time.AddDays(days).ToString()),
          new MySql.Data.MySqlClient.MySqlParameter("?datelogin", time.ToString()),
              new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
              new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)};
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_STILL_LOGINPLAYER, "count", ref stillLoginCount, pars))
        {
            return -1;
        }
        return (decimal)stillLoginCount / newAddCount;
    }



    /// <summary>
    /// 计算time日设备流存率  days:0 当日  -1:昨日  
    /// </summary>
    /// <returns></returns>
    public  decimal GetDeviceRetention(DateTime time, int days)
    {
        DateTime date = time.AddDays(days);
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", date.ToString()),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel",m_channel.Id),
   new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId)};
        //计算当日新增设备数
        Int64 newAddCount = 0;
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_NEWDEVICE, "count", ref newAddCount, array))
        {
            return -1;
        }
        if (newAddCount == 0)
        {
            return 0;
        }
        //计算当日新增设备在第days日依然登陆的设备
        System.Int64 stillLoginCount = 0;

        MySql.Data.MySqlClient.MySqlParameter[] pars = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", time.AddDays(days).ToString()),
        new MySql.Data.MySqlClient.MySqlParameter("?datelogin", time.ToString()),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
          new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId)
                                                        };
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_STILL_LOGINDEVICE, "count", ref stillLoginCount, pars))
        {
            return -1;
        }
        return (decimal)stillLoginCount / newAddCount;
    }

    #endregion


    #region 默认参数
    protected string m_postData;
    protected string m_actionNameURL;
    public string GetActionName()
    {
        return m_actionNameURL;
    }
    protected string m_baseURL = "http://mnreport.chineseall.net/ReportWs.asmx/";
    protected string m_gameName = "我的美女老师";
   // protected string m_gameServerIP = "123.206.200.181:8801";

    protected string m_postSvrInfo="";//要提交的gamesvrip=127.0.0.+"区Id":"渠道"

    public Channel m_channel;       //渠道
    public Svr m_svr;           //区
    
    protected string m_gameServerName = "";
    protected string m_userToken = "c2ab32e4t673802c";
    protected string m_gameCode = "MNLS001";
    protected Encoding m_defaultEncode = Encoding.UTF8;
    protected string m_platform = "中文在线";

    #endregion

    public bool Init(Channel channel,Svr svr)
    {
        m_channel = channel;
        m_svr = svr;
        m_postSvrInfo = m_svr.m_SvrAreaName + ":" + m_channel.ChannelId;
        return true;

    }

    #region 3.2.1	添加游戏
    public bool AddGames()
    {
        m_actionNameURL = "addGames";
        m_postData = "gameName=" + m_gameName + "&gameServerIp=" + m_postSvrInfo +
            "&gameServerName=" + m_svr.m_SvrAreaId+
            "&userToken=" + m_userToken +
            "&gameCode=" + m_gameCode;
        string strError = "";
        if (Post(ref strError) == 0)
        {
            Log.LogError("AddGames failed:" + strError);
            return false;
        }
        return true;
    }
    #endregion




    #region 3.2.2	添加渠道
    public bool AddChannels()
    {
        m_actionNameURL = "addChannels";
        m_postData = "channelName=" + m_channel.Name + "&gameServerIp=" + m_svr.m_SvrAreaName+":"+m_channel.ChannelId+ "&userToken=" + m_userToken;
        string strError = "";
        if (Post(ref strError) == 0)
        {
            Log.LogError("AddGames failed:" + strError);
            return false;
        }
        return true;
    }
    #endregion



    #region //添加运营数据,每隔10分钟统计一次
    public bool AddOperationData(DateTime datetime)
    {
        DataTable tbResult = new DataTable();
        m_actionNameURL = "addOperationData";
        DateTime gaDateTime = datetime;
        Int64 opNewStartDeviceNum = 0;  //新启动设备数
        Int64 opStartDeviceNum = 0;     //启动设备数
        Int64 opAddDeviceNum = 0;       //新增设备数
        Int64 opLoginDeviceNum = 0;     //登录设备数
        Int64 opLoginAccountNum = 0;
        Int64 opFirstLoginAccountNum = 0;
        Int64 opPayAccountNum = 0;
        Int64 opFirstPayAccountNum = 0;
        decimal opIncome = 0m;
        decimal opFirstLoginIncome = 0m;         //todo
        decimal opFirstPayIncome = 0m;
        decimal opPayArpu = 0m;
        decimal opDauArpu = 0m;
        decimal opAccountPayPercent = 0m;
        decimal opFirstLoginPayPercent = 0m;
        string msg = "";
        //今日注册设备数=新启动设备数
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date",gaDateTime) ,
        new MySql.Data.MySqlClient.MySqlParameter("?Channel",m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("SvrAreaId",m_svr.m_SvrAreaId)};
        //今日新登设备数
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_NEWDEVICE, "count", ref opAddDeviceNum, array))
        {
            return false;
        }
        //todo 新启动设备数 = 新登设备数
        opNewStartDeviceNum = opAddDeviceNum;

        //今日登陆设备数
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_LOGINDEVICE, "count", ref opLoginDeviceNum, array))
        {
            return false;
        }
        //todo启动设备= 登陆设备
        opStartDeviceNum = opLoginDeviceNum;
        if (opLoginDeviceNum>0)
        {
            int i = 1;
        }
        //今日登陆账号数
        if (!GetAggregateResult<Int64>(SqlCommand.SELECT_COUNT_LOGINAccount, "count", ref opLoginAccountNum, array))
        {
            return false;
        }

        //今日新登账号数
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_NEWAccount, "count", ref opFirstLoginAccountNum, array))
        {
            return false;
        }

        //获取付费账号数
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_PAY_ACCOUNT);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        //  DateTime time = DateTime.Now.AddDays(-2);        //昨天时间
       // DateTime time = DateTime.Now;        //昨天时间
        st.SetParameter(array);
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        //计算
        opPayAccountNum = tbResult.Rows.Count;
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            try
            {
                decimal sum = (decimal)tbResult.Rows[i]["sum"];
                opIncome += sum;
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
            }
        }
        //新付费账号数
        //获取付费的新玩家
        st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_FIRSTPAY_ACCOUNT);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(array);
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        opFirstPayAccountNum = tbResult.Rows.Count;
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            try
            {
                Decimal sum = (Decimal)tbResult.Rows[i]["sum"];
                opFirstPayIncome += sum; 
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
            }
           
        }
        
        //获取新登账号收入
        if (opFirstLoginAccountNum>0)
        {
            try
            {
                st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_NEWAccount);
                if (st == null)
                {
                    Log.LogError("sql:" + st.GetCommand() + "not register");
                    return false;
                }
                st.SetParameter(array);
                if (!st.Execute(ref tbResult, ref msg))
                {
                    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                    return false;
                }
                //2.获取新登账号的收入
                for (int i = 0; i < tbResult.Rows.Count; ++i)
                {
                    string vopenid = tbResult.Rows[i]["openid"].ToString();
                    //转为ppid
                    string ppid = Util.OpenIdToPPId(vopenid);
                    //查询ppid的今日收入
                    SqlStament stGetSum = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_ACCOUNT_PAY);
                    stGetSum.SetParameter(array);
                    stGetSum.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?ppid", ppid));
                    DataTable tbSum = new DataTable();
                    if (!stGetSum.Execute(ref tbSum, ref msg))
                    {
                        Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                        return false;
                    }
                    // Console.Write(tbSum.Rows[0]["sum"].GetType().ToString());
                    decimal sum = (decimal)tbSum.Rows[0]["sum"];
                    opFirstLoginIncome += sum;
                }
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
            }
        }
   
        if (opPayAccountNum > 0)
        {
            opPayArpu = (decimal)opIncome / opPayAccountNum;
        }
        if (opLoginAccountNum > 0)
        {
            opDauArpu = (decimal)opIncome / opLoginAccountNum;
        }
        if (opLoginAccountNum > 0)
        {
            opAccountPayPercent = (decimal)opPayAccountNum / opLoginAccountNum;
        }
        if (opFirstLoginAccountNum > 0)
        {
            opFirstLoginPayPercent = (decimal)opFirstPayAccountNum / opFirstLoginAccountNum;
        }
        //留存率
        //计算留存率
        int[] days = new int[] { -1, -2, -3, -4, -5, -6, -14, -29 };
        decimal[] accountRetention = new decimal[8];
        for (int i = 0; i < days.Length; ++i)
        {
            var opAccountRetention = GetAccountRetention(gaDateTime, days[i]);
            if (opAccountRetention < 0)
            {
                return false;
            }
            accountRetention[i] = opAccountRetention;
        }
        decimal opYesterdayAccountRetention = accountRetention[0];
        decimal opThreeAccountRetention = accountRetention[1];
        decimal opFourAccountRetention = accountRetention[2];
        decimal opFiveAccountRetention = accountRetention[3];
        decimal opSixAccountRetention = accountRetention[4];
        decimal opSevenAccountRetention = accountRetention[5];
        decimal opFifteenAccountRetention = accountRetention[6];
        decimal opThirtyAccountRetention = accountRetention[7];

        m_postData = "opDateTime=" + gaDateTime +
                    "&gameServerIp=" + m_postSvrInfo +
                    "&userToken=" + m_userToken +
                    "&opPlatform=" + m_platform +
                    "&opNewStartDeviceNum=" + opNewStartDeviceNum.ToString() +
                    "&opStartDeviceNum=" + opStartDeviceNum.ToString() +
                    "&opAddDeviceNum=" + opAddDeviceNum.ToString() +
                    "&opLoginDeviceNum=" + opLoginDeviceNum.ToString() +
                    "&opLoginAccountNum=" + opLoginAccountNum.ToString() +
                    "&opFirstLoginAccountNum=" + opFirstLoginAccountNum.ToString() +
                    "&opPayAccountNum=" + opPayAccountNum.ToString() +
                    "&opFirstPayAccountNum=" + opFirstPayAccountNum.ToString() +
                    "&opIncome=" + opIncome.ToString("#0.00") +
                    "&opFirstLoginIncome=" + opFirstLoginIncome.ToString("#0.00") +
                    "&opFirstPayIncome=" + opFirstPayIncome.ToString("#0.00") +
                    "&opPayArpu=" + opPayArpu.ToString("#0.00") +
                    "&opDauArpu=" + opDauArpu.ToString("#0.00") +
                    "&opAccountPayPercent=" + opAccountPayPercent.ToString("#0.00") +
                    "&opFirstLoginPayPercent=" + opFirstLoginPayPercent.ToString("#0.00") +
                   "&opYesterdayAccountRetention=" + opYesterdayAccountRetention.ToString("#0.00") +
                    "&opThreeAccountRetention=" + opThreeAccountRetention.ToString("#0.00") +
                    "&opFourAccountRetention=" + opFourAccountRetention.ToString("#0.00") +
                    "&opFiveAccountRetention=" + opFiveAccountRetention.ToString("#0.00") +
                    "&opSixAccountRetention=" + opSixAccountRetention.ToString("#0.00") +
                    "&opSevenAccountRetention=" + opSevenAccountRetention.ToString("#0.00") +
                    "&opFifteenAccountRetention=" + opFifteenAccountRetention.ToString("#0.00") +
                    "&opThirtyAccountRetention=" + opThirtyAccountRetention.ToString("#0.00");
        ;
        string strError = "";
        if (Post(ref strError) == 0)
        {
            Log.LogError("AddOperationData failed:" + strError);
            return false;
        }
        return true;
    }
#endregion



#region //账号留存数据 
    public bool AddGamersRetention(DateTime time)
    {
        DataTable tbResult = new DataTable();
        m_actionNameURL = "addGamersRetention";
        DateTime gaDateTime = time;
        Int64 gaRegisterAccountNum = 0;
        Int64 gaLoginAccountNum = 0;
        Int64 gaFirstLoginAccountNum = 0;
        int gaPayAccountNum = 0;
        Decimal ga_income = 0m;
        int gaFirstLoginPayAccountNum = 0;
        Decimal gaFirstLoginPayAccountIncome = 0m;
        Decimal gaAccountPayPercent = 0m;
        Decimal gaFirstLoginAccountPayPercent = 0m;
        Decimal gaFirstLoginAccountPayArpu = 0m;
        //
        decimal gaAcu = 0m;
        Int64 gaPcu = 0;
        string gaPcuTimeInterval = "0";
        Decimal gaAccountAverageOnlineTime = 0m;    //账号平均在线时长
        string msg = "";
        //今日注册账号数
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", gaDateTime) ,
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)                                             };
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_REGAccount, "count", ref gaRegisterAccountNum, array))
        {
            return false;
        }
        //今日登陆账号数
        if (!GetAggregateResult<Int64>(SqlCommand.SELECT_COUNT_LOGINAccount, "count", ref gaLoginAccountNum, array))
        {
            return false;
        }

        //今日新登账号数
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_NEWAccount, "count", ref gaFirstLoginAccountNum, array))
        {
            return false;
        }


        //付费账号数
        //获取今日付费的所有玩家
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_PAY_ACCOUNT);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(array);
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        Dictionary<int, int> countMap = new Dictionary<int, int>();
        //计算
        gaPayAccountNum = tbResult.Rows.Count;
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            try
            {
                decimal sum = (decimal)tbResult.Rows[i]["sum"];
                ga_income += sum;
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
            }        
        }
        //获取付费的新账号
        st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_FIRSTPAY_ACCOUNT);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(array);
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        countMap.Clear();
        gaFirstLoginPayAccountNum = tbResult.Rows.Count;
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            try
            {
                Decimal sum = (Decimal)tbResult.Rows[i]["sum"];
                gaFirstLoginPayAccountIncome += sum;
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
            }
        }
        if (gaLoginAccountNum > 0)
        {
            gaAccountPayPercent = (decimal)gaPayAccountNum / gaLoginAccountNum;
        }


        if (gaFirstLoginAccountNum > 0)
        {
            gaFirstLoginAccountPayPercent = (decimal)gaFirstLoginPayAccountNum / gaFirstLoginAccountNum;
        }
        if (gaFirstLoginPayAccountNum > 0)
        {
            gaFirstLoginAccountPayArpu = (decimal)ga_income / gaFirstLoginPayAccountNum;
        }
        if (gaLoginAccountNum > 0)
        {

            //平均同时在线人数ACU
            if (!GetAggregateResult<decimal>(SqlCommand.SELECT_ACU, "acu", ref gaAcu, array))
            {
                
                return false;
            }
            //gaAcu = gaAcu / 24;
            //最高同时在线人数PCU
            if (!GetAggregateResult<Int64>(SqlCommand.SELECT_PCU, "pcu", ref gaPcu, array))
            {
                return false;
            }
            if (gaPcu != 0)
            {
                gaPcuTimeInterval = Util.CalPCUTimeVal(gaDateTime, m_channel.Id, m_svr.m_SvrAreaId);
            }
            //平均在线时长
                decimal sumOnlineTime = 0;
                //总在线时间
                if (!GetAggregateResult<decimal>(SqlCommand.SELECT_SUM_ONLINETIME, "count", ref sumOnlineTime, array))
                {
                    return false;
                }
                gaAccountAverageOnlineTime = (decimal)sumOnlineTime / gaLoginAccountNum / 60;
        }
       
        //留存率
        //计算留存率
        int[] days = new int[] { -1, -2, -3, -4, -5, -6, -14, -29 };
        decimal[] accountRetention = new decimal[8];
        for (int i = 0; i < days.Length; ++i)
        {
            var opAccountRetention = GetAccountRetention(gaDateTime, days[i]);
            if (opAccountRetention < 0)
            {
                return false;
            }
            accountRetention[i] = opAccountRetention;
        }
        decimal gaYesterdayAccountRetention = accountRetention[0];
        decimal gaThreeAccountRetention = accountRetention[1];
        decimal gaFourAccountRetention = accountRetention[2];
        decimal gaFiveAccountRetention = accountRetention[3];
        decimal gaSixAccountRetention = accountRetention[4];
        decimal gaSevenAccountRetention = accountRetention[5];
        decimal gaFifteenAccountRetention = accountRetention[6];
        decimal gaThirtyAccountRetention = accountRetention[7];

        m_postData = "gaDateTime=" + gaDateTime+
                    "&gameServerIp=" + m_postSvrInfo+
                    "&userToken=" + m_userToken +
                    "&gaPlatform=" + m_platform +
                    "&gaRegisterAccountNum=" + gaRegisterAccountNum.ToString() +
                    "&gaLoginAccountNum=" + gaLoginAccountNum.ToString() +
                    "&gaFirstLoginAccountNum=" + gaFirstLoginAccountNum.ToString() +
                    "&gaPayAccountNum=" + gaPayAccountNum.ToString() +
                    "&ga_income=" + ga_income.ToString("#0.00") +
                    "&gaFirstLoginPayAccountIncome=" + gaFirstLoginPayAccountIncome.ToString("#0.00") +
                    "&gaFirstLoginPayAccountNum=" + gaFirstLoginPayAccountNum.ToString() +
                    "&gaAccountPayPercent=" + gaAccountPayPercent.ToString("#0.00") +
                    "&gaFirstLoginAccountPayPercent=" + gaFirstLoginAccountPayPercent.ToString("#0.00") +
                    "&gaFirstLoginAccountPayArpu=" + gaFirstLoginAccountPayArpu.ToString("#0.00") +
                    "&gaAcu=" + (int)gaAcu +
                    "&gaPcu=" + gaPcu.ToString() +
                    "&gaPcuTimeInterval=" + gaPcuTimeInterval +
                    "&gaAccountAverageOnlineTime=" + gaAccountAverageOnlineTime.ToString("#0.00") +
                    "&gaYesterdayAccountRetention=" + gaYesterdayAccountRetention.ToString("#0.00") +
                    "&gaThreeAccountRetention=" + gaThreeAccountRetention.ToString("#0.00") +
                    "&gaFourAccountRetention=" + gaFourAccountRetention.ToString("#0.00") +
                    "&gaFiveAccountRetention=" + gaFiveAccountRetention.ToString("#0.00") +
                    "&gaSixAccountRetention=" + gaSixAccountRetention.ToString("#0.00") +
                    "&gaSevenAccountRetention=" + gaSevenAccountRetention.ToString("#0.00") +
                    "&gaFifteenAccountRetention=" + gaFifteenAccountRetention.ToString("#0.00") +
                    "&gaThirtyAccountRetention=" + gaThirtyAccountRetention.ToString("#0.00");
        ;
        string strError = "";
        if (Post(ref strError) == 0)
        {
            Log.LogError("AddGamersRetention failed:" + strError);
            return false;
        }
        return true;
    }

    #endregion




#region    //角色留存数据
    public bool AddGamersRoleRetention(DateTime time)
    {
        DataTable tbResult = new DataTable();
        m_actionNameURL = "addGamersRoleRetention";
        DateTime gaDateTime = time;
        Int64 gaRegisterRoleNum = 0;
        Int64 gaLoginRoleNum = 0;
        Int64 gaFirstLoginRoleNum = 0;
        int gaPayRoleNum = 0;
        Decimal ga_income = 0m;
        int gaFirstLoginPayRoleNum = 0;
        Decimal gaFirstLoginPayRoleIncome = 0m;
        Decimal gaRolePayPercent = 0m;
        Decimal gaFirstLoginRolePayPercent = 0m;
        Decimal gaFirstLoginRolePayArpu = 0m;
        //
        decimal gaAcu = 0;
        //UInt32 gaPcu = 0;
        Int64 gaPcu = 0;
        string gaPcuTimeInterval = "0";
        Decimal gaRoleAverageOnlineTime = 0m;    
        //今日注册角色数
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", gaDateTime),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)};
       
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_REGPLAYER, "count", ref gaRegisterRoleNum, array))
        {
            return false;
        }
        //今日登陆角色数
        if (!GetAggregateResult<Int64>(SqlCommand.SELECT_COUNT_LOGINPLAYER, "count", ref gaLoginRoleNum, array))
        {
            return false;
        }

        //今日新登角色数
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_NEWPLAYER, "count", ref gaFirstLoginRoleNum, array))
        {
            return false;
        }


        //付费角色
        //获取今日付费的所有角色
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_PAY_Player);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        //test
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date1", gaDateTime));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date2", gaDateTime));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
        string msg = "";
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        HashSet<UInt64> playerSet = new HashSet<UInt64>();
        try
        {
            for (int i = 0; i < tbResult.Rows.Count; ++i)
            {
                UInt32 sum = (UInt32)tbResult.Rows[i]["price"];
                //付费角色数
                UInt64 playerId = (UInt64)tbResult.Rows[i]["playerid"];
                if (!playerSet.Contains(playerId))
                {
                    //付费角色数+1；
                    playerSet.Add(playerId);
                    gaPayRoleNum++;
                }
                //总收入
                ga_income += sum;
            }
        }
        catch(Exception ex)
        {
            Log.LogError(ex.ToString());
        }
        //获取付费的新角色
        st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_FIRSTPAY_PLAYER);
            if (st == null)
            {
                Log.LogError("sql:" + st.GetCommand() + "not register");
                return false;
            }
            st.SetParameter(array);
            if (!st.Execute(ref tbResult, ref msg))
            {
                Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                return false;
            }
        gaFirstLoginPayRoleNum = tbResult.Rows.Count;
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            try
            {
                Decimal sum = (Decimal)tbResult.Rows[i]["sum"];
                //新登付费角色收入
                gaFirstLoginPayRoleIncome += sum;
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
            }
        }
        if (gaLoginRoleNum > 0)
        {
            //新登付费角色数角色付费率
            gaRolePayPercent = (decimal)gaPayRoleNum / gaLoginRoleNum;
        }


        if (gaFirstLoginRoleNum > 0)
        {
            gaFirstLoginRolePayPercent = (decimal)gaFirstLoginPayRoleNum / gaFirstLoginRoleNum;
        }
        if (gaFirstLoginPayRoleNum > 0)
        {
            gaFirstLoginRolePayArpu = (decimal)ga_income / gaFirstLoginPayRoleNum;
        }

        if (gaLoginRoleNum>0)
        {
            ////平均同时在线人数ACU
            if (!GetAggregateResult<decimal>(SqlCommand.SELECT_ACU, "acu", ref gaAcu, array))
            {
                
                return false;
            }
            //gaAcu = gaAcu / 24;
            //最高同时在线人数PCU
            if (!GetAggregateResult<Int64>(SqlCommand.SELECT_PCU, "pcu", ref gaPcu, array))
            {
                return false;
            }
            if (gaPcu != 0)
            {
                gaPcuTimeInterval = Util.CalPCUTimeVal(gaDateTime, m_channel.Id, m_svr.m_SvrAreaId);
            }
            //平均在线时长
            decimal sumOnlineTime = 0;
            //总在线时间
            if (!GetAggregateResult<decimal>(SqlCommand.SELECT_SUM_ONLINETIME, "count", ref sumOnlineTime, array))
            {
                return false;
            }
            gaRoleAverageOnlineTime = (decimal)sumOnlineTime / gaLoginRoleNum / 60;
        }
        //留存率
        //计算留存率
        int[] days = new int[] { -1, -2, -3, -4, -5, -6, -14, -29 };
        decimal[] accountRetention = new decimal[8];
        for (int i = 0; i < days.Length; ++i)
        {
            var opAccountRetention = GetPlayerRetention(gaDateTime, days[i]);
            if (opAccountRetention < 0)
            {
                return false;
            }
            accountRetention[i] = opAccountRetention;
        }
        decimal gaYesterdayRoleRetention = accountRetention[0];
        decimal gaThreeRoleRetention = accountRetention[1];
        decimal gaFourRoleRetention = accountRetention[2];
        decimal gaFiveRoleRetention = accountRetention[3];
        decimal gaSixRoleRetention = accountRetention[4];
        decimal gaSevenRoleRetention = accountRetention[5];
        decimal gaFifteenRoleRetention = accountRetention[6];
        decimal gaThirtyRoleRetention = accountRetention[7];

        m_postData = "gaDateTime=" + gaDateTime.ToString() +
                    "&gameServerIp=" + m_postSvrInfo+
                    "&userToken=" + m_userToken +
                    "&gaPlatform=" + m_platform +
                    "&gaRegisterRoleNum=" + gaRegisterRoleNum +
                    "&gaLoginRoleNum=" + gaLoginRoleNum +
                    "&gaFirstLoginRoleNum=" + gaFirstLoginRoleNum +
                    "&gaPayRoleNum=" + gaPayRoleNum +
                    "&ga_income=" + ga_income.ToString("#0.00") +
                    "&gaFirstLoginPayRoleIncome=" + gaFirstLoginPayRoleIncome.ToString("#0.00") +
                    "&gaFirstLoginPayRoleNum=" + gaFirstLoginPayRoleNum.ToString() +
                    "&gaRolePayPercent=" + gaRolePayPercent.ToString("#0.00") +
                    "&gaFirstLoginRolePayPercent=" + gaFirstLoginRolePayPercent.ToString("#0.00") +
                    "&gaFirstLoginRolePayArpu=" + gaFirstLoginRolePayArpu.ToString("#0.00") +
                    "&gaAcu=" + (int)gaAcu +
                    "&gaPcu=" + gaPcu.ToString() +
                    "&gaPcuTimeInterval=" + gaPcuTimeInterval +
                    "&gaRoleAverageOnlineTime=" + gaRoleAverageOnlineTime.ToString("#0.00") +
                    "&gaYesterdayRoleRetention=" + gaYesterdayRoleRetention.ToString("#0.00") +
                    "&gaThreeRoleRetention=" + gaThreeRoleRetention.ToString("#0.00") +
                    "&gaFourRoleRetention=" + gaFourRoleRetention.ToString("#0.00") +
                    "&gaFiveRoleRetention=" + gaFiveRoleRetention.ToString("#0.00") +
                    "&gaSixRoleRetention=" + gaSixRoleRetention.ToString("#0.00") +
                    "&gaSevenRoleRetention=" + gaSevenRoleRetention.ToString("#0.00") +
                    "&gaFifteenRoleRetention=" + gaFifteenRoleRetention.ToString("#0.00") +
                    "&gaThirtyRoleRetention=" + gaThirtyRoleRetention.ToString("#0.00");
        ;
        string strError = "";
        if (Post(ref strError) == 0)
        {
            Log.LogError("AddGamersRetention failed:" + strError);
            return false;
        }
        return true;
    }

    #endregion




#region//设备留存数据
    public bool AddGamersEquipRetention(DateTime time)
    {
        DataTable tbResult = new DataTable();
        m_actionNameURL = "addGamersEquipRetention";
        DateTime gaDateTime = time;
        Int64 gaRegisterEquipNum = 0;
        Int64 gaLoginEquipNum = 0;
        Int64 gaFirstLoginEquipNum = 0;
        int gaPayEquipNum = 0;
        Decimal ga_income = 0m;
        Decimal gaFirstLoginPayEquipIncome = 0m;
        int gaFirstLoginPayEquipNum = 0;
        Decimal gaEquipPayPercent = 0m;
        Decimal gaFirstLoginEquipPayPercent = 0m;
        Decimal gaFirstLoginEquipPayArpu = 0m;
        //
        decimal gaAcu = 0;
        Int64 gaPcu = 0;
        string gaPcuTimeInterval = "0";
        Decimal gaEquipAverageOnlineTime = 0m;  //设备平均在线时长
        //今日注册设备数
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
     new MySql.Data.MySqlClient.MySqlParameter("?date", gaDateTime),
    new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)};
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_REGDEVICE, "count", ref gaRegisterEquipNum, array))
        {
            return false;
        }
        //今日登陆设备数
        if (!GetAggregateResult<Int64>(SqlCommand.SELECT_COUNT_LOGINDEVICE, "count", ref gaLoginEquipNum, array))
        {
            return false;
        }

        //今日新登设备数
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_NEWDEVICE, "count", ref gaFirstLoginEquipNum, array))
        {
            return false;
        }


        //付费设备
        //获取今日付费的所有设备。。。。。。。。。todo
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_PAY_ACCOUNT);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(array);
        string msg = "";
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        //todo

        //计算
        gaPayEquipNum = tbResult.Rows.Count;
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            try
            {
                decimal sum = (decimal)tbResult.Rows[i]["sum"];
                ga_income += sum;
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
            }
        }
        //获取付费的新机器
        st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_FIRSTPAY_ACCOUNT);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(array);
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        gaFirstLoginPayEquipNum = tbResult.Rows.Count;
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {

            Decimal sum = (Decimal)tbResult.Rows[i]["sum"];
            gaFirstLoginPayEquipIncome += sum;
        }
        if (gaLoginEquipNum > 0)
        {
            gaEquipPayPercent = (decimal)gaPayEquipNum / gaLoginEquipNum;
        }
        if (gaFirstLoginEquipNum > 0)
        {
            gaFirstLoginEquipPayPercent = (decimal)gaFirstLoginPayEquipNum / gaFirstLoginEquipNum;
        }
        if (gaFirstLoginPayEquipNum > 0)
        {
            gaFirstLoginEquipPayArpu = (decimal)ga_income / gaFirstLoginPayEquipNum;
        }

        if (gaLoginEquipNum>0)
        {
            ////平均同时在线人数ACU
            if (!GetAggregateResult<decimal>(SqlCommand.SELECT_ACU, "acu", ref gaAcu, array))
            {
               
                return false;
            }
            //gaAcu = gaAcu / 24;
            //最高同时在线人数PCU
            if (!GetAggregateResult<Int64>(SqlCommand.SELECT_PCU, "pcu", ref gaPcu, array))
            {
                return false;
            }
            if (gaPcu != 0)
            {
                gaPcuTimeInterval = Util.CalPCUTimeVal(gaDateTime, m_channel.Id, m_svr.m_SvrAreaId);
            }
            //平均在线时长
            decimal sumOnlineTime = 0;
            //总在线时间
            if (!GetAggregateResult<decimal>(SqlCommand.SELECT_SUM_ONLINETIME, "count", ref sumOnlineTime, array))
            {
                return false;
            }
            gaEquipAverageOnlineTime = (decimal)sumOnlineTime / gaLoginEquipNum / 60;
        }
      
        //留存率
        //计算留存率
        int[] days = new int[] { -1, -2, -3, -4, -5, -6, -14, -29 };
        decimal[] accountRetention = new decimal[8];
        for (int i = 0; i < days.Length; ++i)
        {
            var opAccountRetention = GetDeviceRetention(gaDateTime, days[i]);
            if (opAccountRetention < 0)
            {
                return false;
            }
            accountRetention[i] = opAccountRetention;
        }
        decimal gaYesterdayEquipRetention = accountRetention[0];
        decimal gaThreeEquipRetention = accountRetention[1];
        decimal gaFourEquipRetention = accountRetention[2];
        decimal gaFiveEquipRetention = accountRetention[3];
        decimal gaSixEquipRetention = accountRetention[4];
        decimal gaSevenEquipRetention = accountRetention[5];
        decimal gaFifteenEquipRetention = accountRetention[6];
        decimal gaThirtyEquipRetention = accountRetention[7];

        m_postData = "gaDateTime=" + gaDateTime.ToString() +
                    "&gameServerIp=" + m_postSvrInfo+
                    "&userToken=" + m_userToken +
                    "&gaPlatform=" + m_platform +
                    "&gaRegisterEquipNum=" + gaRegisterEquipNum +
                    "&gaLoginEquipNum=" + gaLoginEquipNum +
                    "&gaFirstLoginEquipNum=" + gaFirstLoginEquipNum +
                    "&gaPayEquipNum=" + gaPayEquipNum +
                    "&ga_income=" + ga_income.ToString("#0.00") +
                    "&gaFirstLoginPayEquipIncome=" + gaFirstLoginPayEquipIncome.ToString("#0.00") +
                    "&gaFirstLoginPayEquipNum=" + gaFirstLoginPayEquipNum.ToString() +
                    "&gaEquipPayPercent=" + gaEquipPayPercent.ToString("#0.00") +
                    "&gaFirstLoginEquipPayPercent=" + gaFirstLoginEquipPayPercent.ToString("#0.00") +
                    "&gaFirstLoginEquipPayArpu=" + gaFirstLoginEquipPayArpu.ToString("#0.00") +
                    "&gaAcu=" + (int)gaAcu +
                    "&gaPcu=" + gaPcu.ToString() +
                    "&gaPcuTimeInterval=" + gaPcuTimeInterval +
                    "&gaEquipAverageOnlineTime=" + gaEquipAverageOnlineTime.ToString("#0.00") +
                    "&gaYesterdayEquipRetention=" + gaYesterdayEquipRetention.ToString("#0.00") +
                    "&gaThreeEquipRetention=" + gaThreeEquipRetention.ToString("#0.00") +
                    "&gaFourEquipRetention=" + gaFourEquipRetention.ToString("#0.00") +
                    "&gaFiveEquipRetention=" + gaFiveEquipRetention.ToString("#0.00") +
                    "&gaSixEquipRetention=" + gaSixEquipRetention.ToString("#0.00") +
                    "&gaSevenEquipRetention=" + gaSevenEquipRetention.ToString("#0.00") +
                    "&gaFifteenEquipRetention=" + gaFifteenEquipRetention.ToString("#0.00") +
                    "&gaThirtyEquipRetention=" + gaThirtyEquipRetention.ToString("#0.00");
        ;
        string strError = "";
        if (Post(ref strError) == 0)
        {
            Log.LogError("AddGamersRetention failed:" + strError);
            return false;
        }
        return true;
    }

#endregion


    public class sLTV
    {
        public sLTV(int _day)
        {
            day = _day;
        }
        public int day;
        public decimal ltv = 0m;
        public decimal totalIncome = 0m;
    }


    //获取day天内第一日新增账号day天内付费总额和新增账号
    public bool GetNewAccountDaySumIncome(DateTime time, int days, ref int newaccountNum, ref decimal totalIncome)
    {
        string msg = "";
        DateTime date1 = time.AddDays(-days + 1);
        DataTable newaccountTB = new DataTable();
        //获取当日新登账号
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_NEWAccount);
        if (st == null)
        {
            return false;
        }
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", date1.ToString()));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
           st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
        if (!st.Execute(ref newaccountTB, ref msg))
        {
            return false;
        }
        newaccountNum = newaccountTB.Rows.Count;
        for (int j = 0; j < newaccountTB.Rows.Count; ++j)
        {
            string vopenid = newaccountTB.Rows[j]["openid"].ToString();
            //转为ppid
            string ppid = Util.OpenIdToPPId(vopenid);
            //查询获取新登账号的所有收入
            SqlStament stGetSum = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_LTV_ACCOUNT_PAY);
            if(stGetSum == null)
            {
                return false;
            }
            stGetSum.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date1", date1.ToString()));
            stGetSum.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date2", DateTime.Now.ToString()));
            stGetSum.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?ppid", ppid.ToString()));
            stGetSum.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
            stGetSum.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
            DataTable tbSum = new DataTable();
            if (!stGetSum.Execute(ref tbSum, ref msg))
            {
                return false;
            }
            decimal sum = 0m;
            try
            {
                sum = (decimal)tbSum.Rows[0]["sum"];
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
            }
            totalIncome += sum;
        }
        return true;
    }
#region//常规LTV价值数据 
    public bool AddCommonLtvWorth(DateTime time)
    {
        m_actionNameURL = "addCommonLtvWorth";
        Int64 gaFirstLoginAccountNum = 0;
        //今日新登账号数
        MySql.Data.MySqlClient.MySqlParameter[] newArr = {
 new MySql.Data.MySqlClient.MySqlParameter("?date", time.ToString()),
  new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
  new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),

};
        if (!GetAggregateResult<Int64>(SqlCommand.SELECT_COUNT_NEWAccount, "count", ref gaFirstLoginAccountNum, newArr))
        {
            return false;
        }
        sLTV[] dataArray = new sLTV[] { new sLTV(3), new sLTV(7), new sLTV(15), new sLTV(30) };
        for (int i = 0; i < dataArray.Length; ++i)
        {
            int day = dataArray[i].day;
            decimal totalIncome = 0m;
            int newaccountNum = 0;
            if (!GetNewAccountDaySumIncome( time,day, ref newaccountNum, ref  totalIncome))
            {
                return false;
            }
            //计算ltv
            if (newaccountNum > 0)
            {
                dataArray[i].ltv = totalIncome / newaccountNum;
            }
            dataArray[i].totalIncome = totalIncome;
        }
        bool ret = true;
        string strMsg = "";
        m_postData = "coDateTime=" + time.ToString() +
        "&gameServerIp=" + m_postSvrInfo+
        "&userToken=" + m_userToken +
        "&coPlatform=" + m_platform +
        "&coFirstLoginAccountNum=" + gaFirstLoginAccountNum +
        "&coThreeDayLtv=" + dataArray[0].ltv.ToString("#0.00") +
        "&coSevenDayLtv=" + dataArray[1].ltv.ToString("#0.00") +
        "&coFifteenDayLtv=" + dataArray[2].ltv.ToString("#0.00") +
        "&coThirtyDayLtv=" + dataArray[3].ltv.ToString("#0.00") +
        "&coThreeDayIncome=" + dataArray[0].totalIncome.ToString("#0.00") +
        "&coSevenDayIncome=" + dataArray[1].totalIncome.ToString("#0.00") +
        "&coFifteenDayIncome=" + dataArray[2].totalIncome.ToString("#0.00") +
        "&coThirtyDayIncome=" + dataArray[3].totalIncome.ToString("#0.00");
        if (Post(ref strMsg) == 0)
        {
            Log.LogError("AddCommonLtvWorth failed:" + strMsg);
            ret = false;
        }
        return ret;

    }
#endregion

    public class DayWorth
    {
        public DayWorth(int _day)
        {
            day = _day;
        }
        public int day;
        public Int64 RepeatPayAccountNum = 0;
        public decimal SumIncome = 0m;
    }





#region 3.2.8	添加玩家日价值数据
    public bool AddGamersDayWorth(DateTime time)
    {
        m_actionNameURL = "addGamersDayWorth";
        string msg = "";
        DataTable tbResult = new DataTable();
        //付费用户数
        int gaPayAccountNum = 0;
        //获取今日付费的账号
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_PAY_ACCOUNT);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", time.ToString()));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
          st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        gaPayAccountNum = tbResult.Rows.Count;
        DayWorth[] dataArray = new DayWorth[] { new DayWorth(3), new DayWorth(7), new DayWorth(15), new DayWorth(30) };
        for (int i = 0; i < dataArray.Length; ++i)
        {

            int RepeatPayAccountNum = 0;
            int day = dataArray[i].day;
            DateTime date1 = time.AddDays(-day + 1);
            //查询date1-当期间所有用户的付费次数
            st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_PAY_ACCOUNT_COUNT);
            if (st == null)
            {
                Log.LogError("sql:" + st.GetCommand() + "not register");
                return false;
            }
            st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date1", date1.ToString()));
            st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date2", time.ToString()));
            st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
            st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
            if (!st.Execute(ref tbResult, ref msg))
            {
                Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                return false;
            }
            //计算重复付费账号数
            for (int j = 0; j < tbResult.Rows.Count; ++j)
            {
                try
                {
                    //获取每个Openid的付费次数
                    Int64 payCount = (Int64)tbResult.Rows[j]["count"];
                    if (payCount >= 2)
                    {
                        RepeatPayAccountNum++;
                    }
                }
                catch(Exception ex)
                {
                    Log.LogError(ex.ToString());
                }            
            }
            dataArray[i].RepeatPayAccountNum = RepeatPayAccountNum;
            int newaccountNum = 0;
            decimal SumIncome = 0m;
            //第一日新增账号付费总额
            if (!GetNewAccountDaySumIncome(time, day, ref newaccountNum, ref  SumIncome))
            {
                return false;
            }
            dataArray[i].SumIncome = SumIncome;
        }
        bool ret = true;
        string strMsg = "";
        //提交
        m_postData = "gaDateTime=" + time +
        "&gameServerIp=" +m_postSvrInfo+
        "&userToken=" + m_userToken +
        "&gaPlatform=" + m_platform +
        "&gaPayAccountNum=" + gaPayAccountNum.ToString() +
        "&gaThreeDayRepeatPayAccountNum=" + dataArray[0].RepeatPayAccountNum.ToString() +
        "&gaSevenDayRepeatPayAccountNum=" + dataArray[1].RepeatPayAccountNum.ToString() +
        "&gaFifteenDayRepeatPayAccountNum=" + dataArray[2].RepeatPayAccountNum.ToString() +
        "&gaThirtyDayRepeatPayAccountNum=" + dataArray[3].RepeatPayAccountNum.ToString() +
        "&gaAddThreeDaySumIncome=" + dataArray[0].SumIncome.ToString("#0.00") +
        "&gaAddSevenDaySumIncome=" + dataArray[1].SumIncome.ToString("#0.00") +
        "&gaAddFifteenDaySumIncome=" + dataArray[2].SumIncome.ToString("#0.00") +
        "&gaAddThirtyDaySumIncome=" + dataArray[3].SumIncome.ToString("#0.00");
        if (Post(ref strMsg) == 0)
        {
            Log.LogError("AddCommonLtvWorth failed:" + strMsg);
            ret = false;
        }
        return ret;
    }

#endregion


#region 3.2.9	添加玩家个人充值数据
    //角色个人充值
    public bool AddUserRecharges(DateTime date,DateTime lastTime)
    {
        m_actionNameURL = "addUserRecharges";
        string strMsg = "";
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_AddUserRecharges);
        DataTable resultTb = new DataTable();
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", date.ToString()));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?lastdate",lastTime.ToString()));
        if (!st.Execute(ref resultTb, ref strMsg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + strMsg);
            return false;
        }
        bool ret = true;
        //查询数据库订单状态
        //2: 支付成功PaySuccess，通知成功NotifySuccess
        //0： （date-dtEventTime < 1）下单成功CreateSuccess
        //0:  (date - dtEventTime > 1)支付失败PayFailed
        List<Order> orderList = new List<Order>();

      
        for (int i = 0; i < resultTb.Rows.Count; ++i)
        {
            try
            {
                string orderid = resultTb.Rows[i]["orderid"].ToString();
                string ppid = (string)resultTb.Rows[i]["ppid"].ToString();
                string nickname = resultTb.Rows[i]["nickname"].ToString();
                string playerid = (string)resultTb.Rows[i]["playerid"].ToString();
                string accountCreateTime = resultTb.Rows[i]["accountCreateTime"].ToString();
                DateTime orderCreateTime = (DateTime)resultTb.Rows[i]["orderCreateTime"];
                string orderMoney = resultTb.Rows[i]["price"].ToString();
               // string rechargeMoney = resultTb.Rows[i]["price"].ToString();
                string status = resultTb.Rows[i]["status"].ToString();  
             
               
                //查询渠道订单号
                string channelOrderNo = "";
                int result = SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_AddUserRecharges_ChannelOrderNo, "pforderid", ref channelOrderNo, new MySql.Data.MySqlClient.MySqlParameter("?orderid", orderid));
               if(result<0)
               {
                   return false;
               }
               Order order = new Order(orderid, ppid, nickname, playerid, accountCreateTime, orderCreateTime.ToString(), decimal.Parse(orderMoney).ToString("#0.00"), channelOrderNo);
                if (status == "2")
                {
                    order.m_orderState = OrderState.NotifySuccess;
                    order.m_rechargeState = rechargetState.Payed;
                    Order order2 = new Order(orderid, ppid, nickname, playerid, accountCreateTime, orderCreateTime.ToString(), decimal.Parse(orderMoney).ToString("#0.00"), channelOrderNo);
                    order2.m_orderState = OrderState.PaySuccess;
                    order2.m_rechargeState = rechargetState.Payed;
                    orderList.Add(order2);
                }
                else if (status == "0")
                {
                    TimeSpan span = (TimeSpan)(date - orderCreateTime);
                    //todo
                    if (span.Minutes < 1)
                   {
                         order.m_orderState = OrderState.CreateSuccess;
                   }
                    else
                    {
                        order.m_orderState = OrderState.PayFailed;
                    }
                    order.m_rechargeState = rechargetState.NoPayed;
                }
                else
                {
                    order.m_orderState = OrderState.Other;
                }
                orderList.Add(order);
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
                continue;
            }
        }
        //if(orderList.Count == 0)
        //{
        //    Order order = new Order("0", "0", "0", "#0.00", date);
        //    order.m_orderState = OrderState.NoData;
        //    orderList.Add(order);
        //}
        foreach (var order in orderList)
        {
            m_postData =
      "gameOrderNo=" + order.m_orderid.ToString() +
       "&channelOrderNo=" + order.m_channelOrderNo.ToString() +
     "&gameServerIp=" + m_postSvrInfo +
 "&userToken=" + m_userToken +
  "&gamerAccount=" + order.m_ppid +
  "&roleName=" + order.m_userNickname +
  "&roleId=" + order.m_playerid +
  "&mainChannelName=" + "棱镜" +
"&equipId=" + "" +
"&platform=" + m_platform+
"&gamerServerName=" + m_svr.m_SvrAreaId +
"&accountCreateTime=" + order.m_accountCreateTime +
"&gamerPhone=" + "" +
"&orderCreateTime=" + order.m_orderCreateTime +
"&orderMoney=" + order.m_money +
"&rechargeMoney=" + order.m_money +
"&rechargeFunc=" + "" +
 "&rechargetState=" + (int)order.m_rechargeState +
 "&orderState=" +(int) order.m_orderState;
            if (Post(ref strMsg) == 0)
            {
                Log.LogError("AddUserRecharges failed:" + strMsg);
                ret = false;
            }
        }
        return ret;
    }

#endregion

#region //批量提交
    public bool PostBatch<T>(List<T> infoList)
    {
        if (infoList.Count > 0)
        {
            try
            {
                string jsonBatch = new JavaScriptSerializer().Serialize(infoList);
                //批量添加等级流失数据
                string msg = "";
                m_postData = "userToken=" + m_userToken +
                               "&jsonBatch=" + jsonBatch;
                if (Post(ref msg) == 0)
                {
                    Log.LogError("PostBatch "+infoList.GetType().ToString()+ "failed:" + msg);
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
                return false;
            }
        }
        return true;
    }
#endregion

#region 3.2.10	批量添加每日新增玩家等级分布
    public bool AddEveryDayAddGamerLevelFenbu(DateTime time)
    {
        DateTime evDateTime = time;
        DataTable tbResult = new DataTable();
        Dictionary<Int64, int> levelMap = new Dictionary<Int64, int>();
        m_actionNameURL = "addEverydayAddGamerLevelFenbuBatch";
        //SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_NEWRPLAYER_LEVEL);
        //if (st == null)
        //{
        //    Log.LogError("sql:" + st.GetCommand() + "not register");
        //    return false;
        //}
        //st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", evDateTime));
        //st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
        //st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
        //string msg = "";
        //if (!st.Execute(ref tbResult, ref msg))
        //{
        //    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
        //    return false;
        //}

        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWRPLAYER_LEVEL, ref tbResult,
            new MySql.Data.MySqlClient.MySqlParameter("?date", evDateTime),
            new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
            new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)) < 0)
        {
            return false;
        }
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            try
            {
                var str = tbResult.Rows[i]["level"].GetType().ToString();
                Int64 level = (Int64)tbResult.Rows[i]["level"];
                if (levelMap.ContainsKey(level))
                {
                    levelMap[level]++;
                }
                else
                {
                    levelMap[level] = 1;
                }
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
            }
           
        }
        List<EverydayAddGamerLevelFenbuInfo> infoList = new List<EverydayAddGamerLevelFenbuInfo>();
        foreach (var date in levelMap)
        {
            Int64 ilevel = date.Key;
            int icount = date.Value;
            //m_postData = "evDateTime=" + evDateTime +
            //    "&gameServerIp=" + m_postSvrInfo +
            //"&userToken=" + m_userToken +
            //"&evGamerLevel=" + ilevel.ToString() +
            //"&evGamerNumber=" + icount.ToString();
            //if (Post(ref strMsg) == 0)
            //{
            //    Log.LogError("AddEveryDayAddGamerLevelFenbu failed:" + strMsg);
            //    ret = false;
            //}
            EverydayAddGamerLevelFenbuInfo info = new EverydayAddGamerLevelFenbuInfo();
            info.evDateTime = evDateTime.ToString();
            info.gameServerIp = m_postSvrInfo;
            info.evGamerLevel = ilevel;
            info.evGamerNumber = icount;
            infoList.Add(info);
        }
        return PostBatch<EverydayAddGamerLevelFenbuInfo>(infoList);
    }
    #endregion



#region 3.2.11	批量添加所有玩家等级分布
    public bool AddAllGemerLevelFenbu(DateTime time)
    {
        //m_actionNameURL = "addAllGamerLevelFenbu";
        m_actionNameURL = "addAllGamerLevelFenbuBatch";
        string msg = "";
        DataTable tbResult = new DataTable();
        Dictionary<Int64, int> levelMap = new Dictionary<Int64, int>();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_ALLPLAYER_LEVEL, ref tbResult, 
            new MySql.Data.MySqlClient.MySqlParameter("?date", time.ToString()),
             new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
             new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId))<0)
        {
            return false;
        }
        try
        {
            for (int i = 0; i < tbResult.Rows.Count; ++i)
            {
                var str = tbResult.Rows[i]["level"].GetType().ToString();
                Int64 level = (Int64)tbResult.Rows[i]["level"];
                if (levelMap.ContainsKey(level))
                {
                    levelMap[level]++;
                }
                else
                {
                    levelMap[level] = 1;
                }
            }
        }
        catch (Exception ex)
        {
            Log.LogError(ex.ToString());
        }
        List<AllGamerLevelFenbuInfo> infoList = new List<AllGamerLevelFenbuInfo>();
        foreach (var date in levelMap)
        {
            Int64 ilevel = date.Key;
            int icount = date.Value;
            AllGamerLevelFenbuInfo info = new AllGamerLevelFenbuInfo();
            info.allDateTime = time.ToString();
            info.gameServerIp = m_postSvrInfo;
            info.allGamerLevel = ilevel;
            info.allGamerNumber = icount;
            infoList.Add(info);
            //m_postData = "allDateTime=" + time +
            //"&gameServerIp=" + m_postSvrInfo +
            //"&userToken=" + m_userToken +
            //"&allGamerLevel=" + ilevel.ToString() +
            //"&allGamerNumber=" + icount.ToString();
            //if (Post(ref msg) == 0)
            //{
            //    Log.LogError("addAllGamerLevelFenbu failed:" + msg);
            //    ret = false;
            //}

        }
        return PostBatch<AllGamerLevelFenbuInfo>(infoList);
    }
#endregion
#region 3.2.12	批量添加等级变更数据

    public bool AddGamerLevelChanges(DateTime time)
    {
        //m_actionNameURL = "addGamerLevelChanges";
        m_actionNameURL = "addGamerLevelChangesBatch";
        DateTime chDateTime = time.AddDays(-1);
        //获取昨日升级的所有角色
        DataTable tbYesLevel = new DataTable();
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_ALLPLAYER_LEVELUP);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", time.AddDays(0)));
        string msg = "";
        if (!st.Execute(ref tbYesLevel, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        //获取chDateTime所有角色等级
        DataTable allRoleTb = new DataTable();
        st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_ALLPLAYER_LEVEL);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", chDateTime));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
        if (!st.Execute(ref allRoleTb, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return false;
        }
        Dictionary<Int64, int> levelMap = new Dictionary<Int64, int>();
        try
        {
            DataTable noLevelUpTb = new DataTable();
            if(allRoleTb.Rows.Count > 0)
            {
                IEnumerable<DataRow> query = allRoleTb.AsEnumerable().Except(tbYesLevel.AsEnumerable(), new PlayerDataRowComparer());
                //两个数据源的差集集合
                if (query.Count() != 0)
                {
                    noLevelUpTb = query.CopyToDataTable();
                }
            }
            for (int i = 0; i < noLevelUpTb.Rows.Count; ++i)
            {
                Int64 level = (Int64)noLevelUpTb.Rows[i]["level"];
                if (levelMap.ContainsKey(level))
                {
                    levelMap[level]++;
                }
                else
                {
                    levelMap[level] = 1;
                }
            }

        }
        catch (Exception ex)
        {
            Log.LogError(ex.ToString());
            //return false;
        }
        List<GamerLevelChangeInfo> infoList = new List<GamerLevelChangeInfo>();
        foreach (var levelItem in levelMap)
        {
            Int64 ilevel = levelItem.Key;
            int icount = levelItem.Value;
            GamerLevelChangeInfo info = new GamerLevelChangeInfo();
            info.chDateTime = chDateTime.ToString();
            info.gameServerIp = m_postSvrInfo;
            info.chGamerLevel = ilevel;
            info.chGamerNumber = icount;
            infoList.Add(info);
            //m_postData = "chDateTime=" + chDateTime +
            //    "&gameServerIp=" + m_postSvrInfo +
            //"&userToken=" + m_userToken +
            //"&chGamerLevel=" + ilevel.ToString() +
            //"&chGamerNumber=" + icount.ToString();
            //if (Post(ref strMsg) == 0)
            //{
            //    Log.LogError("AddGamerLevelChanges failed:" + strMsg);
            //    ret = false;
            //}
        }
        return PostBatch<GamerLevelChangeInfo>(infoList);
    }
    #endregion

#region 3.2.13	批量添加等级流失数据
    public bool AddGamerLevelLeft(DateTime time)
    {
        //7.1-(7.2-7.8(今天))
       // m_actionNameURL = "addGamerLevelLeft";
        m_actionNameURL = "addGamerLevelLeftBatch";
        //提交日志的时间
        DateTime leDateTime = time.AddDays(-7);
        string msg = "";
        //获取leDateTime当日的登录用户
        DataTable logRoleTb = new DataTable();
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_LOGINPLAYER);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return false;
        }
        MySql.Data.MySqlClient.MySqlParameter[] pars = {
        new MySql.Data.MySqlClient.MySqlParameter("?date", leDateTime),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
         new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
        };
        st.SetParameter(pars);
        if (!st.Execute(ref logRoleTb, ref msg))
        {
            return false;
        }
        //(7.2-7.8(今天))的登录用户
        DataTable[] tbArray = new DataTable[7];
        DataTable unionTB = new DataTable();
      
        DataTable lostTB = new DataTable();
        //计算流失玩家的等级记录
        Dictionary<Int64, int> levelMap = new Dictionary<Int64, int>();
        try
        {
            for (int i = 0; i < 7; ++i)
            {
                st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_LOGINPLAYER);
                if (st == null)
                {
                    Log.LogError("sql:" + st.GetCommand() + "not register");
                    return false;
                }
                DateTime date = time.AddDays(0 - i);
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", date));
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
                if (!st.Execute(ref tbArray[i], ref msg))
                {
                    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                    return false;
                }
                if (tbArray[i].Rows.Count > 0)
                {
                    IEnumerable<DataRow> unionTBROW = unionTB.AsEnumerable().Union(tbArray[i].AsEnumerable(), new PlayerDataRowComparer());
                    if(unionTBROW.Count() != 0)
                    {
                        unionTB = unionTBROW.CopyToDataTable();
                    }
                }
            }
            if (logRoleTb.Rows.Count > 0 && unionTB.Rows.Count > 0)
            {
                //计算流失玩家
                IEnumerable<DataRow> lostTBROW = logRoleTb.AsEnumerable().Except(unionTB.AsEnumerable(), new PlayerDataRowComparer());
                if (lostTBROW.Count() != 0)
                {
                    lostTB = lostTBROW.CopyToDataTable();
                }
            }
            for (int i = 0; i < lostTB.Rows.Count; ++i)
            {
                DataTable tbResult = new DataTable();
                var vopenid = lostTB.Rows[i]["vopenid"];
                var SvrAreaId = lostTB.Rows[i]["SvrAreaId"];
                var dtEventTime = lostTB.Rows[i]["dtEventTime"];
                //查询vopenid,SvrAreaId对应的玩家等级信息
                st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_PLAYER_LEVEL);
                if (st == null)
                {
                    Log.LogError("sql:" + st.GetCommand() + "not register");
                    return false;
                }
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?vopenid", vopenid));
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", SvrAreaId));
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", dtEventTime));

                if (!st.Execute(ref tbResult, ref msg))
                {
                    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                    
                }
                //没有升级,1J
                if (tbResult.Rows.Count <= 0)
                {
                    if (levelMap.ContainsKey(1))
                    {
                        levelMap[1]++;
                    }
                    else
                    {
                        levelMap[1] = 1;
                    }

                }
                else
                {
                    Int32 level = (Int32)tbResult.Rows[0]["level"];
                        if (levelMap.ContainsKey(level))
                        {
                            levelMap[level]++;
                        }
                        else
                        {
                            levelMap[level] = 1;
                        }
                }
            }
        }
        catch(Exception ex)
        {
            Log.LogError(ex.ToString());
            //return false;
        }
        List<GamerLevelLeftInfo> infoList = new List<GamerLevelLeftInfo>();
        foreach (var date in levelMap)
        {
            Int64 ilevel = date.Key;
            int icount = date.Value;
            GamerLevelLeftInfo info = new GamerLevelLeftInfo();
            info.leDateTime = leDateTime.ToString();
            info.gameServerIp = m_postSvrInfo;
            info.leGamerLevel = ilevel;
            info.leGamerNumber = icount;
            infoList.Add(info);
            //m_postData = "leDateTime=" + leDateTime +
            //    "&gameServerIp=" + m_postSvrInfo+
            //"&userToken=" + m_userToken +
            //"&leGamerLevel=" + ilevel.ToString() +
            //"&leGamerNumber=" + icount.ToString();
            //if (Post(ref msg) == 0)
            //{
            //    Log.LogError("addGamerLevelChanges failed:" + msg);
            //    ret = false;
            //}
        }
        return PostBatch<GamerLevelLeftInfo>(infoList);
    }

#endregion

public class CornCost
    {
        public CornCost(string costName, Int64 costNum)
        {
            coCostCategoryName = costName;
            coCostNumber = costNum;
        }
        public string coCostCategoryName;
        public Int64 coCostNumber = 0;
    }

#region	批量添加虚拟币消耗数据 

    public Dictionary<string, DiamondPay> m_diamondPayMap = new Dictionary<string, DiamondPay>();

    public Dictionary<string, DiamondPresent> m_diamondPresentMap = new Dictionary<string, DiamondPresent>();
    public bool AddVirtualCornCost(DateTime time)
    {
        DateTime coDateTime = time;
        m_actionNameURL = "addVirtualCornCostBatch";
        //角色消费钻石DiamondPayRecord
        Int64 payNum = 0;
        Int64 preNum = 0;
        MySql.Data.MySqlClient.MySqlParameter[] pars = {
        new MySql.Data.MySqlClient.MySqlParameter("?date", coDateTime),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId), };
        DataTable payTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_DIAMONDPAY, ref payTB, pars) < 0)
        {
            return false;
        }
        DataTable preTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_DIAMONDPRESENT, ref preTB, pars) < 0)
        {
            return false;
        }
        Dictionary<string, int> payTBMap = new Dictionary<string, int>();
        for (int i = 0; i < payTB.Rows.Count; ++i)
        {
            string DiamondPayItemType;
            try
            {
                DiamondPayItemType = payTB.Rows[i]["DiamondBuyItemType"].ToString();
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
                continue;
            }
            if(payTBMap.ContainsKey(DiamondPayItemType))
            {
                payTBMap[DiamondPayItemType]++;
            }
            else
            {
                payTBMap[DiamondPayItemType]=1;
            }
        }


        Dictionary<string, int> preTBMap = new Dictionary<string, int>();
        for (int i = 0; i < preTB.Rows.Count; ++i)
        {
            string EventID="";
            try
            {
                EventID = preTB.Rows[i]["EventID"].ToString();
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
                continue;
            }
            if (preTBMap.ContainsKey(EventID))
            {
                preTBMap[EventID]++;
            }
            else
            {
                preTBMap[EventID] = 1;
            }
        }


        List<VirtualCornCostInfo> infoList = new List<VirtualCornCostInfo>();
        foreach(var pay in payTBMap)
        {
            string itemType = pay.Key;
            if (!m_diamondPayMap.ContainsKey(itemType))
            {
                Log.LogError("not find diamondpayType:" + itemType);
                continue;
            }
            else
            {
                string coCostCategoryName = m_diamondPayMap[itemType].desc;
                int coCostNumber = pay.Value;
                VirtualCornCostInfo virCost = new VirtualCornCostInfo();
                virCost.coDateTime = time.ToString();
                virCost.gameServerIp = m_postSvrInfo;
                virCost.coCostCategoryName = coCostCategoryName;
                virCost.coCostNumber = coCostNumber;
                infoList.Add(virCost);
            }
        }

        foreach (var present in preTBMap)
        {
            string eventID = present.Key;
            if (!preTBMap.ContainsKey(eventID))
            {
                Log.LogError("not find diamondpreType:" + eventID);
                continue;
            }
            else
            {
                string coCostCategoryName = m_diamondPresentMap[eventID].desc;
                int coCostNumber = present.Value;
                VirtualCornCostInfo virCost = new VirtualCornCostInfo();
                virCost.coDateTime = time.ToString();
                virCost.gameServerIp = m_postSvrInfo;
                virCost.coCostCategoryName = coCostCategoryName;
                virCost.coCostNumber = coCostNumber;
                infoList.Add(virCost);
            }
        }
        return PostBatch<VirtualCornCostInfo>(infoList);
            
    }
#endregion

# region  批量添加鲸鱼数据
    public bool AddRechargeGamerInfo(DateTime time)
    {
        //test delete
        m_actionNameURL = "addRechargeGamerInfoBatch";
        string inEquip = ""; 
        //获取今日付费的所有玩家
        DataTable tbResult = new DataTable();
        //SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_PAY_Player);
        //if (st == null)
        //{
        //    Log.LogError("sql:" + st.GetCommand() + "not register");
        //    return false;
        //}
        //st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date1", time));
        //st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date2", time));
        //st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
        //st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId));
        //if (!st.Execute(ref tbResult, ref msg))
        //{
        //    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
        //    return false;
        //}

        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PAY_Player, ref tbResult, new MySql.Data.MySqlClient.MySqlParameter("?date1", time), new MySql.Data.MySqlClient.MySqlParameter("?date2", time),
           new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
           new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)) < 0)
        {
            return false;
        }
        List<BigRechargeGamerInfo> infoList = new List<BigRechargeGamerInfo>();
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            DataRow dataRow = tbResult.Rows[i];
            try
            {
                UInt64 ppid = (UInt64)dataRow["ppid"];
                string inRoleName = (string)dataRow["nickname"];
                var playerid = dataRow["playerid"];
                DateTime timestamp = (DateTime)dataRow["timestamp"];
                string inRechargeMoney = "";
                //获得所有的充值金额
                if(SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_RechargeGamerInfo, "sum",
                    ref inRechargeMoney,
                    new MySql.Data.MySqlClient.MySqlParameter("?date", time),
                    new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
                    new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
                    new MySql.Data.MySqlClient.MySqlParameter("?playerid", playerid))<0)
                {
                    return false;
                }
                UInt32 SvrAreaId = (UInt32)dataRow["zoneid"];
                //当日钻石拥有数量
                UInt32 inVirtualCornOwnNumber = (UInt32)dataRow["diamond"];
                string inRechargeLevel = "1";
                //获得openid
                string openid = Util.PPIdToOpenId((Int64)ppid);
                DataTable tbGamer = new DataTable();
                //最后一次的登录device
                if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_RECHARGEPLAYER_DEVICEID, "SystemHardware",
                ref inEquip,
                new MySql.Data.MySqlClient.MySqlParameter("?openid", openid),
                new MySql.Data.MySqlClient.MySqlParameter("?date", time),
                new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
                new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)) < 0)
                {
                    return false;
                }

                //最后一次升级的等级
                if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_RECHARGEPLAYER_LEVEL, "AfterLevel",
               ref inRechargeLevel,
               new MySql.Data.MySqlClient.MySqlParameter("?openid", openid),
               new MySql.Data.MySqlClient.MySqlParameter("?date", time),
               new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)) < 0)
                {
                    return false;
                }
                //钻石消耗数量
                string inVirtualCornSumCost = "0";
                if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_RECHARGEPLAYER_DIAMOND_CONSUMCOST, "sum",
             ref inVirtualCornSumCost,
             new MySql.Data.MySqlClient.MySqlParameter("?openid", openid),
             new MySql.Data.MySqlClient.MySqlParameter("?date", time),
             new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId)) < 0)
                {
                    return false;
                }
                BigRechargeGamerInfo info = new BigRechargeGamerInfo();
                info.caculateDateTime = time.ToString();
                info.inRoleName = inRoleName;
                info.gameServerIp = m_postSvrInfo;
                info.inRoleId = playerid.ToString();
                info.inEquip = inEquip;
                info.inRechargeMoney = (Convert.ToDecimal(inRechargeMoney)).ToString("#0.00");
                info.inRechargeLevel = inRechargeLevel.ToString();
                info.inDateTime = timestamp.ToString();
                info.inVirtualCornOwnNumber = inVirtualCornOwnNumber;
                info.inVirtualCornSumCost = inVirtualCornSumCost;
                infoList.Add(info);
               
                //m_postData = 
                //    "caculateDateTime="+time+
                //    "&inRoleName=" + inRoleName +
                //"&gameServerIp=" + m_postSvrInfo+
                //"&userToken=" + m_userToken +
                //"&inRoleId=" + playerid +
                //"&inEquip=" + inEquip +
                //"&inRechargeMoney=" + (Convert.ToDecimal(inRechargeMoney)).ToString("#0.00") +
                //"&inRechargeLevel=" + inRechargeLevel +
                //"&inDateTime=" + timestamp.ToString() +
                //"&inVirtualCornOwnNumber=" + inVirtualCornOwnNumber +
                //"&inVirtualCornSumCost=" + inVirtualCornSumCost;
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
            }
            //if (Post(ref msg) == 0)
            //{
            //    Log.LogError("AddRechargeGamerInfo failed:" + msg);
            //    ret = false;
            //}
        }
        return PostBatch<BigRechargeGamerInfo>(infoList);
    }

#endregion
}



