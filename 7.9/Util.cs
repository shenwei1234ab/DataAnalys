using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
using System.Configuration;
class Util
{

    //int 转为string的点分十进制表示(256->127.0.1.0)
    public static string ConvertToDotDecim(UInt32 num)
    {
        String ret = "127.";
        //转为6位16进制
        string s = num.ToString("X6");
        //分割16进制字符串
        char[] array = s.ToCharArray();
        for (int i = 0; i < 6; i = i + 2)
        {

            string strVal = array[i].ToString() + array[i + 1].ToString();
            //转为10进制
            int dotVal = Convert.ToInt32(strVal, 16);
            //转为字符串
            ret += dotVal.ToString();
            if (i != 6 - 2)
            {
                ret += ".";
            }
        }
        return ret;
    }

    public static string GetIPString(UInt32 iSvrId)
    {
        return Util.ConvertToDotDecim(iSvrId);
    }


    public static string Get64Bit(Int64 intnum)
    {
        byte[] bytes = BitConverter.GetBytes(intnum);
        return Convert.ToBase64String(bytes);
    }

    public static string DecodeBase64(string result)
    {
        byte[] outputb = Convert.FromBase64String(result);
        long orgStr = BitConverter.ToInt64(outputb, 0);

        return orgStr.ToString();
    }

  
    public static string OpenIdToPPId(string vopenid)
    {
        return DecodeBase64(vopenid);
    }


    //base64编码
    public static string PPIdToOpenId(Int64 ppid)
    {
        return Get64Bit(ppid);
    }


    //计算今日pcu时段
    public static string CalPCUTimeVal(DateTime date, string Channel, uint SvrAreaId)
    {
        string msg="";
        string ret = "0-0";
        SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_ALL_ONLINECNT);
        if (st == null)
        {
            Log.LogError("sql:" + st.GetCommand() + "not register");
            return ret;
        }
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?date", date.ToString()));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", Channel.ToString()));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", SvrAreaId.ToString()));
        DataTable tbResult = new DataTable();
        if (!st.Execute(ref tbResult, ref msg))
        {
            Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
            return ret;
        }
        try
        {
            if (tbResult.Rows.Count <=0)
            {
                return ret;
            }
            DateTime dtMin = (DateTime)tbResult.Rows[0]["dtEventTime"];
            PcuTimeval tmpPCU = new PcuTimeval(dtMin,0);
            List<PcuTimeval> pcuList = new List<PcuTimeval>();
            for (int i = 0; i < tbResult.Rows.Count; ++i)
            {
                DateTime datetime = (DateTime)tbResult.Rows[i]["dtEventTime"];
                UInt32 onlineNum = (UInt32)tbResult.Rows[i]["OnlineCnt"];
                if(datetime > dtMin.AddHours(1))
                {
                    dtMin = datetime;
                    tmpPCU = new PcuTimeval(dtMin, onlineNum);
                    pcuList.Add(tmpPCU);
                }
                else
                {
                    tmpPCU.m_num += onlineNum;
                }
            }
            //排序
            pcuList.Sort();
            //输出最大的时间
            int hour = pcuList[0].m_pcuTime.Hour;
            int  nexthour = pcuList[0].m_pcuTime.AddHours(1).Hour;
            ret = hour.ToString() + "点-" + nexthour.ToString() + "点";
        }
        catch(Exception ex)
        {
            Log.LogError(ex.ToString());
            return ret;
        }
        return ret;
    }

}
