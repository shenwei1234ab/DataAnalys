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
public class EveryDayTask: Task
{
  
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
new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId),
new MySql.Data.MySqlClient.MySqlParameter("?OsId",m_platform.m_platformId)
                                                        };
        //计算date日新登账号数
        DataTable newAddAccountTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWAccount, ref newAddAccountTB, array) < 0)
        {
            return -1;
        }
        int newAddCount = newAddAccountTB.Rows.Count;
        if (newAddCount == 0)
        {
            return 0;
        }
        //计算time日登陆账号
        MySql.Data.MySqlClient.MySqlParameter[] array2 = { 
new MySql.Data.MySqlClient.MySqlParameter("?date", time),
new MySql.Data.MySqlClient.MySqlParameter("?Channel",m_channel.Id),
new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId),
new MySql.Data.MySqlClient.MySqlParameter("?OsId",m_platform.m_platformId)};
        DataTable logAccountTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_LOGINAccount, ref logAccountTB, array2) < 0)
        {
            return -1;
        }
        if(logAccountTB.Rows.Count <= 0 )
        {
            return 0;
        }
        //求交集
        DataTable stillLogTB = new DataTable();
        try
        {
            IEnumerable<DataRow> query = newAddAccountTB.AsEnumerable().Intersect(logAccountTB.AsEnumerable(), new AccountDataRowComparer());
            //两个数据源的交集集合
            if (query.Count() != 0)
            {
                stillLogTB = query.CopyToDataTable();
            }
        }
        catch(Exception ex)
        {
            Log.LogError(ex.ToString());
            return -1;
        }
        return (decimal)stillLogTB.Rows.Count / newAddCount;
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
           new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId),
           new MySql.Data.MySqlClient.MySqlParameter("?OsId",m_platform.m_platformId)
        };
        //计算date日新增角色
        DataTable newAddPlayerTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWPLAYER, ref newAddPlayerTB, array) < 0)
        {
            return -1;
        }
        Int64 newAddCount = newAddPlayerTB.Rows.Count;
        if (newAddCount == 0)
        {
            return 0;
        }
        //计算time日登陆的角色
        DataTable logPlayerTB = new DataTable();
         MySql.Data.MySqlClient.MySqlParameter[] array2 = 
        { 
            new MySql.Data.MySqlClient.MySqlParameter("?date", time),
            new MySql.Data.MySqlClient.MySqlParameter("?Channel",m_channel.Id),
           new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId),
           new MySql.Data.MySqlClient.MySqlParameter("?OsId",m_platform.m_platformId)
        };
        if(SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_LOGINPLAYER,ref logPlayerTB,array2)<0)
        {
            return -1;
        }
        //计算交集newAddPlayerTB&logPlayerTB
        if(logPlayerTB.Rows.Count<=0)
        {
            return 0;
        }
        DataTable stillLogTB = new DataTable();
        try
        {
            IEnumerable<DataRow> query = newAddPlayerTB.AsEnumerable().Intersect(logPlayerTB.AsEnumerable(), new PlayerDataRowComparer());
            //两个数据源的交集集合
            if (query.Count() != 0)
            {
                stillLogTB = query.CopyToDataTable();
            }
        }
        catch (Exception ex)
        {
            Log.LogError(ex.ToString());
            return -1;
        }

        System.Int64 stillLoginCount = stillLogTB.Rows.Count;
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
   new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId),
 new MySql.Data.MySqlClient.MySqlParameter("?OsId",m_platform.m_platformId)};
        //计算date当日新增设备
        Int64 newAddCount = 0;
        DataTable newAddCountTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWDEVICE, ref newAddCountTB,array)<0)
        {
            return -1;
        }
        newAddCount = newAddCountTB.Rows.Count;
        if (newAddCount <= 0)
        {
            return 0;
        }
        //计算time当日登陆设备
        MySql.Data.MySqlClient.MySqlParameter[] array2 = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", time.ToString()),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel",m_channel.Id),
   new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId",m_svr.m_SvrAreaId),
 new MySql.Data.MySqlClient.MySqlParameter("?OsId",m_platform.m_platformId)};
        DataTable timeLogTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_LOGINDEVICE, ref timeLogTB, array2) < 0)
        {
            return -1;
        }
        if(timeLogTB.Rows.Count <= 0)
        {
            return 0;
        }

        DataTable stillLogTB = new DataTable();
        try
        {
            IEnumerable<DataRow> query = newAddCountTB.AsEnumerable().Intersect(timeLogTB.AsEnumerable(), DataRowComparer.Default);
            //两个数据源的交集集合
            if (query.Count() != 0)
            {
                stillLogTB = query.CopyToDataTable();
            }
        }
        catch (Exception ex)
        {
            Log.LogError(ex.ToString());
            return -1;
        }


        System.Int64 stillLoginCount = stillLogTB.Rows.Count;
        return (decimal)stillLoginCount / newAddCount;
    }

    #region 默认参数

    protected string m_postSvrInfo="";//要提交的gamesvrip=127.0.0.+"区Id":"渠道"
    public Channel m_channel;       //渠道
    public Svr m_svr;           //区
    public Platform m_platform; //平台（android ios）

    #endregion

    public virtual bool Init(Channel channel, Svr svr, Platform platform)
    {
        m_channel = channel;
        m_svr = svr;
        m_postSvrInfo = m_svr.m_SvrAreaName + ":" + m_channel.ChannelId;
        m_firstlogPayPercent = 0m;
        m_platform = platform;
        return true;
    }



   //预处理 在Init后调用
    DataTable m_prePayPlayerTB = new DataTable();

    public bool PreQuery(DateTime datetime)
    {
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", datetime),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),             new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)};
        //查询今日付费角色
        //获取今日付费的所有角色
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PAY_Player, ref m_prePayPlayerTB, array) < 0)
        {
            Log.LogError("SELECT_PAY_Player failed");
            return false;
        }
        return true;
    }

    #region 3.2.1	添加游戏
    public bool AddGames()
    {
        m_actionNameURL = "addGames";
        m_postData = "gameName=" + m_gameName + "&gameServerIp=" + m_svr.m_SvrAreaName +
            "&gameServerName=" + m_svr.m_SvrAreaId+
            "&userToken=" + m_userToken +
            "&gameCode=" + m_gameCode;
        if (Post() == 0)
        {
            Log.LogError("AddGames failed:");
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
        if (Post() == 0)
        {
            Log.LogError("AddGames failed:" );
            return false;
        }
        return true;
    }
    #endregion



   

    //todo 新登角色付费率 = 新登账号付费率 = 新登机器付费率
    private decimal m_firstlogPayPercent = 0;
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
        //今日注册账号数
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", gaDateTime) ,
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
           new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)};
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_REGAccount, "count", ref gaRegisterAccountNum, array))
        {
            return false;
        }
        //今日登陆账号数
        DataTable LogTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_LOGINAccount, ref LogTB, array) < 0)
        {
            return false;
        }
        gaLoginAccountNum = LogTB.Rows.Count;
        //今日新登账号数
        DataTable newLogTB = new DataTable();
        if(SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWAccount,ref newLogTB,array)<0)
        {
            return false;
        }
        gaFirstLoginAccountNum = newLogTB.Rows.Count;
        //付费账号数
        //获取今日付费的所有玩家
        if(SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PAY_ACCOUNT,ref tbResult,array) <0 )
        {
            return false;
        }

        //计算新登账号付费率（今日新登账号里付费的账号）
        DataTable newLogPayTb = new DataTable();
        if(newLogTB.Rows.Count >0 && tbResult.Rows.Count>0)
        {
            //newLogTB 的 openid 转为ppid
           //netlogtb 添加新的一列(为了与tbResult的ppid比较)
            DataColumn newCol = new DataColumn("ppid");
            newLogTB.Columns.Add(newCol);
            for(int i=0;i<newLogTB.Rows.Count;++i)
            {
                try{
                    string openid = newLogTB.Rows[i]["openid"].ToString();
                    string ppid = Util.OpenIdToPPId(openid);
                    newLogTB.Rows[i]["ppid"] = ppid;
                }
                catch(Exception ex)
                {
                    Log.LogError(ex.ToString());
                }
            }
            IEnumerable<DataRow> newLogPay = newLogTB.AsEnumerable().Except(tbResult.AsEnumerable(), DataRowComparer.Default);
            if (newLogPay.Count() != 0)
            {
                newLogPayTb = newLogPay.CopyToDataTable();
            }
        }
        if (newLogPayTb.Rows.Count > 0 && gaFirstLoginAccountNum > 0)
        {
            gaFirstLoginAccountPayPercent = (decimal) newLogPayTb.Rows.Count / gaFirstLoginAccountNum;
            m_firstlogPayPercent = gaFirstLoginAccountPayPercent;
            if(m_firstlogPayPercent > 1)
            {
                Log.LogError("calculate firstlogPayPercent failed");
                m_firstlogPayPercent = 1m;
            }
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
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_FIRSTPAY_ACCOUNT, ref tbResult, array) < 0)
        {
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
            if(gaAccountPayPercent>1)
            {
                Log.LogError("calculate gaAccountPayPercent error");
                gaAccountPayPercent = 1m;
            }
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
        string strFormat = "#0.0000";
        m_postData = "gaDateTime=" + gaDateTime+
                    "&gameServerIp=" + m_postSvrInfo+
                    "&userToken=" + m_userToken +
                    "&gaPlatform=" + m_platform.m_platformName +
                    "&gaRegisterAccountNum=" + gaRegisterAccountNum.ToString() +
                    "&gaLoginAccountNum=" + gaLoginAccountNum.ToString() +
                    "&gaFirstLoginAccountNum=" + gaFirstLoginAccountNum.ToString() +
                    "&gaPayAccountNum=" + gaPayAccountNum.ToString() +
                    "&ga_income=" + ga_income.ToString(strFormat) +
                    "&gaFirstLoginPayAccountIncome=" + gaFirstLoginPayAccountIncome.ToString(strFormat) +
                    "&gaFirstLoginPayAccountNum=" + gaFirstLoginPayAccountNum.ToString() +
                    "&gaAccountPayPercent=" + gaAccountPayPercent.ToString(strFormat) +
                    "&gaFirstLoginAccountPayPercent=" + gaFirstLoginAccountPayPercent.ToString(strFormat) +
                    "&gaFirstLoginAccountPayArpu=" + gaFirstLoginAccountPayArpu.ToString(strFormat) +
                    "&gaAcu=" + (int)gaAcu +
                    "&gaPcu=" + gaPcu.ToString() +
                    "&gaPcuTimeInterval=" + gaPcuTimeInterval +
                    "&gaAccountAverageOnlineTime=" + gaAccountAverageOnlineTime.ToString(strFormat) +
                    "&gaYesterdayAccountRetention=" + gaYesterdayAccountRetention.ToString(strFormat) +
                    "&gaThreeAccountRetention=" + gaThreeAccountRetention.ToString(strFormat) +
                    "&gaFourAccountRetention=" + gaFourAccountRetention.ToString(strFormat) +
                    "&gaFiveAccountRetention=" + gaFiveAccountRetention.ToString(strFormat) +
                    "&gaSixAccountRetention=" + gaSixAccountRetention.ToString(strFormat) +
                    "&gaSevenAccountRetention=" + gaSevenAccountRetention.ToString(strFormat) +
                    "&gaFifteenAccountRetention=" + gaFifteenAccountRetention.ToString(strFormat) +
                    "&gaThirtyAccountRetention=" + gaThirtyAccountRetention.ToString(strFormat);
        ;
        if (Post() == 0)
        {
            Log.LogError("AddGamersRetention failed:");
            return false;
        }
        return true;
    }

    #endregion




#region    //角色留存数据
    public bool AddGamersRoleRetention(DateTime time)
    {
       // DataTable tbResult = new DataTable();
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
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),             new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)};
       
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
        //付费角色,m_prePayPlayerTB
        //获取今日付费的所有角色
        try
        {
            for (int i = 0; i < m_prePayPlayerTB.Rows.Count; ++i)
            {
                decimal sum = (decimal)m_prePayPlayerTB.Rows[i]["sum"];
                gaPayRoleNum++;
                //总收入
                ga_income += sum;
            }
        }
        catch(Exception ex)
        {
            Log.LogError(ex.ToString());
        }
        //今日之前已经付费的角色
        DataTable payBeforeTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PAY_Player_BEFORE, ref payBeforeTB, array) < 0)
        {
            Log.LogError("SELECT_PAY_Player failed");
            return false;
        }
        //今日第一次付费角色 = 今日付费角色-今日之前付费的角色
        DataTable resultTB = new DataTable();
        if (m_prePayPlayerTB.Rows.Count > 0)
        {
            IEnumerable<DataRow> query = m_prePayPlayerTB.AsEnumerable().Except(payBeforeTB.AsEnumerable(), DataRowComparer.Default);
            //两个数据源的差集集合
            if (query.Count() != 0)
            {
                resultTB = query.CopyToDataTable();
            }
        }
        gaFirstLoginPayRoleNum = resultTB.Rows.Count;
        for (int i = 0; i < resultTB.Rows.Count; ++i)
        {
            try
            {
                Decimal sum = (Decimal)resultTB.Rows[i]["sum"];
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
            if(gaRolePayPercent>1)
            {
                Log.LogError("calculate gaRolePayPercent error");
                gaRolePayPercent = 1m;
            }
        }


        if (gaFirstLoginRoleNum > 0)
        {
            gaFirstLoginRolePayPercent = m_firstlogPayPercent;
            if(gaFirstLoginRolePayPercent>1)
            {
                Log.LogError("calculate gaFirstLoginRolePayPercent error");
                gaFirstLoginRolePayPercent = 1m;
            }
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
        string strFormat = "#0.0000";
        m_postData = "gaDateTime=" + gaDateTime.ToString() +
                    "&gameServerIp=" + m_postSvrInfo+
                    "&userToken=" + m_userToken +
                    "&gaPlatform=" + m_platform.m_platformName +
                    "&gaRegisterRoleNum=" + gaRegisterRoleNum +
                    "&gaLoginRoleNum=" + gaLoginRoleNum +
                    "&gaFirstLoginRoleNum=" + gaFirstLoginRoleNum +
                    "&gaPayRoleNum=" + gaPayRoleNum +
                    "&ga_income=" + ga_income.ToString(strFormat) +
                    "&gaFirstLoginPayRoleIncome=" + gaFirstLoginPayRoleIncome.ToString(strFormat) +
                    "&gaFirstLoginPayRoleNum=" + gaFirstLoginPayRoleNum.ToString() +
                    "&gaRolePayPercent=" + gaRolePayPercent.ToString(strFormat) +
                    "&gaFirstLoginRolePayPercent=" + gaFirstLoginRolePayPercent.ToString(strFormat) +
                    "&gaFirstLoginRolePayArpu=" + gaFirstLoginRolePayArpu.ToString(strFormat) +
                    "&gaAcu=" + (int)gaAcu +
                    "&gaPcu=" + gaPcu.ToString() +
                    "&gaPcuTimeInterval=" + gaPcuTimeInterval +
                    "&gaRoleAverageOnlineTime=" + gaRoleAverageOnlineTime.ToString(strFormat) +
                    "&gaYesterdayRoleRetention=" + gaYesterdayRoleRetention.ToString(strFormat) +
                    "&gaThreeRoleRetention=" + gaThreeRoleRetention.ToString(strFormat) +
                    "&gaFourRoleRetention=" + gaFourRoleRetention.ToString(strFormat) +
                    "&gaFiveRoleRetention=" + gaFiveRoleRetention.ToString(strFormat) +
                    "&gaSixRoleRetention=" + gaSixRoleRetention.ToString(strFormat) +
                    "&gaSevenRoleRetention=" + gaSevenRoleRetention.ToString(strFormat) +
                    "&gaFifteenRoleRetention=" + gaFifteenRoleRetention.ToString(strFormat) +
                    "&gaThirtyRoleRetention=" + gaThirtyRoleRetention.ToString(strFormat);
        ;
        if (Post() == 0)
        {
            Log.LogError("AddGamersRetention failed:");
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
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),             new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)};
        if (!GetAggregateResult<System.Int64>(SqlCommand.SELECT_COUNT_REGDEVICE, "count", ref gaRegisterEquipNum, array))
        {
            return false;
        }
        //今日登陆设备数
        DataTable logdeviceTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_LOGINDEVICE, ref logdeviceTB,array)<0)
        {
            return false;
        }
        gaLoginEquipNum = logdeviceTB.Rows.Count;
       
        DataTable newlogdeviceTB=new DataTable();
        //今日新登设备数
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWDEVICE, ref newlogdeviceTB, array) < 0)
        {
            return false;
        }
        gaFirstLoginEquipNum = newlogdeviceTB.Rows.Count;

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
            if(gaEquipPayPercent>1)
            {
                Log.LogError("calculate gaEquipPayPercent error");
                gaEquipPayPercent = 1m;
            }
        }
        if (gaFirstLoginEquipNum > 0)
        {
            gaFirstLoginEquipPayPercent = m_firstlogPayPercent;
            if(gaFirstLoginEquipPayPercent>1)
            {
                Log.LogError("calculate gaFirstLoginEquipPayPercent error");
                gaFirstLoginEquipPayPercent = 1m;
            }
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
        string strFormat = "#0.0000";
        m_postData = "gaDateTime=" + gaDateTime.ToString() +
                    "&gameServerIp=" + m_postSvrInfo+
                    "&userToken=" + m_userToken +
                    "&gaPlatform=" + m_platform.m_platformName +
                    "&gaRegisterEquipNum=" + gaRegisterEquipNum +
                    "&gaLoginEquipNum=" + gaLoginEquipNum +
                    "&gaFirstLoginEquipNum=" + gaFirstLoginEquipNum +
                    "&gaPayEquipNum=" + gaPayEquipNum +
                    "&ga_income=" + ga_income.ToString("#0.00") +
                    "&gaFirstLoginPayEquipIncome=" + gaFirstLoginPayEquipIncome.ToString("#0.00") +
                    "&gaFirstLoginPayEquipNum=" + gaFirstLoginPayEquipNum.ToString() +
                    "&gaEquipPayPercent=" + gaEquipPayPercent.ToString(strFormat) +
                    "&gaFirstLoginEquipPayPercent=" + gaFirstLoginEquipPayPercent.ToString(strFormat) +
                    "&gaFirstLoginEquipPayArpu=" + gaFirstLoginEquipPayArpu.ToString(strFormat) +
                    "&gaAcu=" + (int)gaAcu +
                    "&gaPcu=" + gaPcu.ToString() +
                    "&gaPcuTimeInterval=" + gaPcuTimeInterval +
                    "&gaEquipAverageOnlineTime=" + gaEquipAverageOnlineTime.ToString(strFormat) +
                    "&gaYesterdayEquipRetention=" + gaYesterdayEquipRetention.ToString(strFormat) +
                    "&gaThreeEquipRetention=" + gaThreeEquipRetention.ToString(strFormat) +
                    "&gaFourEquipRetention=" + gaFourEquipRetention.ToString(strFormat) +
                    "&gaFiveEquipRetention=" + gaFiveEquipRetention.ToString(strFormat) +
                    "&gaSixEquipRetention=" + gaSixEquipRetention.ToString(strFormat) +
                    "&gaSevenEquipRetention=" + gaSevenEquipRetention.ToString(strFormat) +
                    "&gaFifteenEquipRetention=" + gaFifteenEquipRetention.ToString(strFormat) +
                    "&gaThirtyEquipRetention=" + gaThirtyEquipRetention.ToString(strFormat);
        ;
        if (Post() == 0)
        {
            Log.LogError("AddGamersRetention failed:");
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
          MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", date1),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),             new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)};
          if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWAccount, ref newaccountTB, array) < 0)
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
            stGetSum.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId));
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
        MySql.Data.MySqlClient.MySqlParameter[] array = {
 new MySql.Data.MySqlClient.MySqlParameter("?date", time.ToString()),
  new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
  new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
   new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)
};
        DataTable newAccountTb= new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWAccount, ref newAccountTb, array)<0)
        {
            return false;
        }
        gaFirstLoginAccountNum = newAccountTb.Rows.Count;
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
        string strFormat = "#0.00";
        m_postData = "coDateTime=" + time.ToString() +
        "&gameServerIp=" + m_postSvrInfo+
        "&userToken=" + m_userToken +
        "&coPlatform=" + m_platform.m_platformName +
        "&coFirstLoginAccountNum=" + gaFirstLoginAccountNum +
        "&coThreeDayLtv=" + dataArray[0].ltv.ToString(strFormat) +
        "&coSevenDayLtv=" + dataArray[1].ltv.ToString(strFormat) +
        "&coFifteenDayLtv=" + dataArray[2].ltv.ToString(strFormat) +
        "&coThirtyDayLtv=" + dataArray[3].ltv.ToString(strFormat) +
        "&coThreeDayIncome=" + dataArray[0].totalIncome.ToString(strFormat) +
        "&coSevenDayIncome=" + dataArray[1].totalIncome.ToString(strFormat) +
        "&coFifteenDayIncome=" + dataArray[2].totalIncome.ToString(strFormat) +
        "&coThirtyDayIncome=" + dataArray[3].totalIncome.ToString(strFormat);
        if (Post() == 0)
        {
            Log.LogError("AddCommonLtvWorth failed:" );
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
        MySql.Data.MySqlClient.MySqlParameter[] array = {
 new MySql.Data.MySqlClient.MySqlParameter("?date", time.ToString()),
  new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
  new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
   new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)
};
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PAY_ACCOUNT, ref tbResult, array) < 0)
        {
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
            MySql.Data.MySqlClient.MySqlParameter[] array2 = {
 new MySql.Data.MySqlClient.MySqlParameter("?date1", date1.ToString()),
  new MySql.Data.MySqlClient.MySqlParameter("?date2", time.ToString()),
  new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
  new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
   new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)
};

            if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PAY_ACCOUNT_COUNT, ref tbResult, array2) < 0)
            {
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
        string strFormat = "#0.00";
        //提交
        m_postData = "gaDateTime=" + time +
        "&gameServerIp=" +m_postSvrInfo+
        "&userToken=" + m_userToken +
        "&gaPlatform=" + m_platform.m_platformName +
        "&gaPayAccountNum=" + gaPayAccountNum.ToString() +
        "&gaThreeDayRepeatPayAccountNum=" + dataArray[0].RepeatPayAccountNum.ToString() +
        "&gaSevenDayRepeatPayAccountNum=" + dataArray[1].RepeatPayAccountNum.ToString() +
        "&gaFifteenDayRepeatPayAccountNum=" + dataArray[2].RepeatPayAccountNum.ToString() +
        "&gaThirtyDayRepeatPayAccountNum=" + dataArray[3].RepeatPayAccountNum.ToString() +
        "&gaAddThreeDaySumIncome=" + dataArray[0].SumIncome.ToString(strFormat) +
        "&gaAddSevenDaySumIncome=" + dataArray[1].SumIncome.ToString(strFormat) +
        "&gaAddFifteenDaySumIncome=" + dataArray[2].SumIncome.ToString(strFormat) +
        "&gaAddThirtyDaySumIncome=" + dataArray[3].SumIncome.ToString(strFormat);
        if (Post() == 0)
        {
            Log.LogError("AddCommonLtvWorth failed:" );
            ret = false;
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
                m_postData = "userToken=" + m_userToken +
                               "&jsonBatch=" + jsonBatch;
                if (Post() == 0)
                {
                    Log.LogError("PostBatch "+infoList.GetType().ToString()+ "failed:" );
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
        MySql.Data.MySqlClient.MySqlParameter[] array = {
 new MySql.Data.MySqlClient.MySqlParameter("?date", evDateTime),
new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)
};

        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWRPLAYER_LEVEL, ref tbResult,array) < 0)
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
        if(SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_ALLPLAYER_LEVELUP,ref tbYesLevel,
            new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
            new MySql.Data.MySqlClient.MySqlParameter("?date", time.AddDays(0)),
            new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)
            )<0)
        {
            return false;
        }
        //获取chDateTime所有角色等级
        DataTable allRoleTb = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_ALLPLAYER_LEVEL, ref allRoleTb, 
            new MySql.Data.MySqlClient.MySqlParameter("?date", chDateTime),
            new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id), 
            new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
             new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)) < 0)
        {
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
        MySql.Data.MySqlClient.MySqlParameter[] pars = {
        new MySql.Data.MySqlClient.MySqlParameter("?date", leDateTime),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
        new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId),
        };
        if(SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_LOGINPLAYER,ref logRoleTb,pars )<0)
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

                SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_LOGINPLAYER);
                if (st == null)
                {
                    Log.LogError("sql:" + st.GetCommand() + "not register");
                    return false;
                }
                DateTime date = time.AddDays(0 - i);
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", date));
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id));
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId));
                st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId));
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
                if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PLAYER_LEVEL, ref tbResult, 
                    new MySql.Data.MySqlClient.MySqlParameter("?vopenid", vopenid),
                    new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", SvrAreaId),
                    new MySql.Data.MySqlClient.MySqlParameter("?date", dtEventTime),
                    new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)) < 0)
                {
                    return false;
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
        MySql.Data.MySqlClient.MySqlParameter[] pars = {
        new MySql.Data.MySqlClient.MySqlParameter("?date", coDateTime),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId), 
        new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)};
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
                 string coCostCategoryName="";
                if (!m_diamondPayMap.ContainsKey(itemType))
                {
                    Log.LogError("diamondPayMap not contains" + itemType);
                }
                else
                {
                    coCostCategoryName = m_diamondPayMap[itemType].desc;
                }
                
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
                string coCostCategoryName = "";
                if (!m_diamondPresentMap.ContainsKey(eventID))
                {
                    Log.LogError("diamondPresentMap not contains" + eventID);
                }
                else
                {
                     coCostCategoryName = m_diamondPresentMap[eventID].desc;
                }
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
        //获取今日付费所有玩家
        DataTable tbResult = new DataTable();
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", time),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
        new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)
};

        //今日付费所有玩家
        List<string> payList = new List<string>();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PAY_Player, ref tbResult,array) < 0)
        {
            return false;
        }
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            DataRow dataRow = tbResult.Rows[i];
            try
            {
                if (dataRow["sum"] != null && dataRow["playerid"] != null)
                {
                    payList.Add(dataRow["playerid"].ToString());
                }
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
            }
            continue;
        }
        List<BigRechargeGamerInfo> infoList = new List<BigRechargeGamerInfo>();
        foreach (var playerid in payList)
        {
            //获取鲸鱼用户数据
            if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_RechargeGamerInfo, ref tbResult,
                new MySql.Data.MySqlClient.MySqlParameter("?date", time),
               new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
               new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
                 new MySql.Data.MySqlClient.MySqlParameter("?playerid", playerid)) < 0)
            {
                return false;
            }
            for (int i = 0; i < tbResult.Rows.Count; ++i)
            {
                DataRow dataRow = tbResult.Rows[i];
                try
                {
                    UInt64 ppid = (UInt64)dataRow["ppid"];
                    string inRoleName = (string)dataRow["nickname"];
                    //var playerid = dataRow["playerid"];
                    DateTime timestamp = (DateTime)dataRow["timestamp"];
                    string inRechargeMoney = dataRow["sum"].ToString();
                    //UInt32 SvrAreaId = (UInt32)dataRow["zoneid"];
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
                    new MySql.Data.MySqlClient.MySqlParameter("?date", timestamp),
                    new MySql.Data.MySqlClient.MySqlParameter("?Channel", m_channel.Id),
                    new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
                     new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)) < 0)
                    {
                        return false;
                    }

                    //最后一次升级的等级
                    if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_RECHARGEPLAYER_LEVEL, "AfterLevel",
                   ref inRechargeLevel,
                   new MySql.Data.MySqlClient.MySqlParameter("?openid", openid),
                   new MySql.Data.MySqlClient.MySqlParameter("?date", timestamp),
                   new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
                     new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)) < 0)
                    {
                        return false;
                    }
                    //钻石消耗数量
                    string inVirtualCornSumCost = "0";
                    if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_RECHARGEPLAYER_DIAMOND_CONSUMCOST, "sum",
                 ref inVirtualCornSumCost,
                 new MySql.Data.MySqlClient.MySqlParameter("?openid", openid),
                 new MySql.Data.MySqlClient.MySqlParameter("?date", timestamp),
                 new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", m_svr.m_SvrAreaId),
                 new MySql.Data.MySqlClient.MySqlParameter("?OsId", m_platform.m_platformId)) < 0)
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
                }
                catch (Exception ex)
                {
                    Log.LogError(ex.ToString());
                }
            }
        }
        return PostBatch<BigRechargeGamerInfo>(infoList);
    }

#endregion
}



