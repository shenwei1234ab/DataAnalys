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



public class AddOperationDataTask:AnalysisSystem
{
    #region //添加运营数据,每隔10分钟统计一次


    public override void RunTask(DateTime time)
    {
        Log.SetFileName("AddOperationDataTask");
        Log.LogDebug("AddOperationDataTask Start");
        foreach (var taskdata in m_taskData)
        {
            AddOperationData(time, taskdata);
        }
        Log.LogDebug("AddOperationDataTask End");
    }

    public bool AddOperationData(DateTime datetime,TaskData data)
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
        //今日注册设备数=新启动设备数
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date",gaDateTime) ,
        new MySql.Data.MySqlClient.MySqlParameter("?Channel",data.m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("SvrAreaId",data.m_svr.m_SvrAreaId)};
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
        //今日登陆账号数
        DataTable logAccountTB= new DataTable();
        if(SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_LOGINAccount,ref logAccountTB,array)<0)
        {
            return false;
        }
        opLoginAccountNum = logAccountTB.Rows.Count;
        //今日新登账号数
        DataTable newLogTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWAccount, ref newLogTB, array) < 0)
        {
            return false;
        }
        opFirstLoginAccountNum = newLogTB.Rows.Count;


        //付费账号数
        //获取今日付费的所有玩家
        DataTable payaccountTB = new DataTable();
        if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_PAY_ACCOUNT, ref payaccountTB, array) < 0)
        {
            return false;
        }
        //计算
        opPayAccountNum = payaccountTB.Rows.Count;
        for (int i = 0; i < payaccountTB.Rows.Count; ++i)
        {
            try
            {
                decimal sum = (decimal)payaccountTB.Rows[i]["sum"];
                opIncome += sum;
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
            }
        }
        //计算新登账号付费率（今日新登账号 && 付费账号）
        DataTable newLogPayTb = new DataTable();
        if (newLogTB.Rows.Count > 0 && payaccountTB.Rows.Count > 0)
        {
            //newLogTB 的 openid 转为ppid
            //netlogtb 添加新的一列(为了与payaccountTB的ppid比较)
            DataColumn newCol = new DataColumn("ppid");
            newCol.DataType = typeof(UInt64);
            newLogTB.Columns.Add(newCol);

            for (int i = 0; i < newLogTB.Rows.Count; ++i)
            {
                try
                {
                    string openid = newLogTB.Rows[i]["openid"].ToString();
                    string ppid = Util.OpenIdToPPId(openid);
                    UInt64 uppid = Convert.ToUInt64(ppid);

                    newLogTB.Rows[i]["ppid"] = uppid;
                }
                catch (Exception ex)
                {
                    Log.LogError(ex.ToString());
                }
            }
            IEnumerable<DataRow> newLogPay = newLogTB.AsEnumerable().Intersect(payaccountTB.AsEnumerable(), new PayAccountDataRowComparer());
            if (newLogPay.Count() != 0)
            {
                newLogPayTb = newLogPay.CopyToDataTable();
            }
        }

        opFirstPayAccountNum = newLogPayTb.Rows.Count;
        if (newLogPayTb.Rows.Count > 0 && opFirstLoginAccountNum > 0)
        {
            opFirstLoginPayPercent = (decimal)newLogPayTb.Rows.Count / opFirstLoginAccountNum;
            if(opFirstLoginPayPercent > 1)
            {
                opFirstLoginPayPercent = 1m;
                Log.LogError("calculate opFirstLoginPayPercent failed");
            }
        }
        for (int i = 0; i < newLogPayTb.Rows.Count; ++i)
        {
            try
            {
                Decimal sum = (Decimal)newLogPayTb.Rows[i]["sum"];
                opFirstPayIncome += sum;
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
            }
        }
    
        //新付费账号数
        //获取付费的新玩家
        //if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_FIRSTPAY_ACCOUNT, ref tbResult, array) < 0)
        //{
        //    return false;
        //}
        //opFirstPayAccountNum = tbResult.Rows.Count;
        //for (int i = 0; i < tbResult.Rows.Count; ++i)
        //{
        //    try
        //    {
        //        Decimal sum = (Decimal)tbResult.Rows[i]["sum"];
        //        opFirstPayIncome += sum;
        //    }
        //    catch (Exception ex)
        //    {
        //        Log.LogError(ex.ToString());
        //    }

        //}

        //获取新登账号收入
        if (opFirstLoginAccountNum > 0)
        {
            try
            {
                if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_NEWAccount, ref tbResult, array) < 0)
                {
                    return false;
                }
                //2.获取新登账号的收入
                for (int i = 0; i < tbResult.Rows.Count; ++i)
                {
                    string vopenid = tbResult.Rows[i]["openid"].ToString();
                    //转为ppid
                    string ppid = Util.OpenIdToPPId(vopenid);
                    //查询ppid的今日收入
                    DataTable tbSum = new DataTable();
                    if (SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_ACCOUNT_PAY, ref tbSum, new MySql.Data.MySqlClient.MySqlParameter("?date", gaDateTime),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel", data.m_channel.Id),
        new MySql.Data.MySqlClient.MySqlParameter("SvrAreaId", data.m_svr.m_SvrAreaId),
        new MySql.Data.MySqlClient.MySqlParameter("?ppid", ppid)) < 0)
                    {
                        return false;
                    }

                    // Console.Write(tbSum.Rows[0]["sum"].GetType().ToString());
                    decimal sum = (decimal)tbSum.Rows[0]["sum"];
                    opFirstLoginIncome += sum;
                }
            }
            catch (Exception ex)
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
        //if (opFirstLoginAccountNum > 0)
        //{
        //    opFirstLoginPayPercent = (decimal)opFirstPayAccountNum / opFirstLoginAccountNum;
        //}
        //留存率
        //计算留存率
        int[] days = new int[] { -1, -2, -3, -4, -5, -6, -14, -29 };
        decimal[] accountRetention = new decimal[8];
        for (int i = 0; i < days.Length; ++i)
        {
            var opAccountRetention = GetAccountRetention(gaDateTime, days[i], data);
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
        string strFormat = "#0.0000";
        m_postData = "opDateTime=" + gaDateTime +
                    "&gameServerIp=" + data.m_postSvrInfo +
                    "&userToken=" + m_userToken +
                    "&opPlatform=" + data.m_channel.Platform +
                    "&opNewStartDeviceNum=" + opNewStartDeviceNum.ToString() +
                    "&opStartDeviceNum=" + opStartDeviceNum.ToString() +
                    "&opAddDeviceNum=" + opAddDeviceNum.ToString() +
                    "&opLoginDeviceNum=" + opLoginDeviceNum.ToString() +
                    "&opLoginAccountNum=" + opLoginAccountNum.ToString() +
                    "&opFirstLoginAccountNum=" + opFirstLoginAccountNum.ToString() +
                    "&opPayAccountNum=" + opPayAccountNum.ToString() +
                    "&opFirstPayAccountNum=" + opFirstPayAccountNum.ToString() +
                    "&opIncome=" + opIncome.ToString(strFormat) +
                    "&opFirstLoginIncome=" + opFirstLoginIncome.ToString(strFormat) +
                    "&opFirstPayIncome=" + opFirstPayIncome.ToString(strFormat) +
                    "&opPayArpu=" + opPayArpu.ToString(strFormat) +
                    "&opDauArpu=" + opDauArpu.ToString(strFormat) +
                    "&opAccountPayPercent=" + opAccountPayPercent.ToString(strFormat) +
                    "&opFirstLoginPayPercent=" + opFirstLoginPayPercent.ToString(strFormat) +
                   "&opYesterdayAccountRetention=" + opYesterdayAccountRetention.ToString(strFormat) +
                    "&opThreeAccountRetention=" + opThreeAccountRetention.ToString(strFormat) +
                    "&opFourAccountRetention=" + opFourAccountRetention.ToString(strFormat) +
                    "&opFiveAccountRetention=" + opFiveAccountRetention.ToString(strFormat) +
                    "&opSixAccountRetention=" + opSixAccountRetention.ToString(strFormat) +
                    "&opSevenAccountRetention=" + opSevenAccountRetention.ToString(strFormat) +
                    "&opFifteenAccountRetention=" + opFifteenAccountRetention.ToString(strFormat) +
                    "&opThirtyAccountRetention=" + opThirtyAccountRetention.ToString(strFormat);
        ;
        if (Post() == 0)
        {
            Log.LogError("AddOperationData failed:");
            return false;
        }
        return true;
    }
    #endregion

}
