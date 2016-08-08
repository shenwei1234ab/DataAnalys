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
        string ret = "0-0";
        MySql.Data.MySqlClient.MySqlParameter[] array = { 
        new MySql.Data.MySqlClient.MySqlParameter("?date", date.ToString()),
        new MySql.Data.MySqlClient.MySqlParameter("?Channel",Channel.ToString()),
        new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", SvrAreaId.ToString())};
        DataTable tbResult = new DataTable();
        if(SqlManager.GetInstance().SetAndExecute(SqlCommand.SELECT_ALL_ONLINECNT,ref tbResult,array)<0)
        {
            Log.LogError("SELECT_ALL_ONLINECNT error");
            return ret;
        }
            if (tbResult.Rows.Count <=0)
            {
                return ret;
            }

         Dictionary<int, UInt32> onlineMap = new Dictionary<int, UInt32>();
           // DateTime dtMin = (DateTime)tbResult.Rows[0]["dtEventTime"];
            //PcuTimeval tmpPCU = new PcuTimeval(dtMin,0);
           // List<PcuTimeval> pcuList = new List<PcuTimeval>();
           // pcuList.Add(tmpPCU);
        for (int i = 0; i < tbResult.Rows.Count; ++i)
        {
            try
            {
                DateTime datetime = (DateTime)tbResult.Rows[i]["dtEventTime"];
                int hour = datetime.Hour;
                UInt32 onlineNum = (UInt32)tbResult.Rows[i]["OnlineCnt"];
                if (onlineMap.ContainsKey(hour))
                 {
                     onlineMap[hour] += onlineNum;
                 }
                 else
                 {
                     onlineMap[hour] = onlineNum;
                 }
            }
            catch(Exception ex)
            {
                Log.LogError(ex.ToString());
                continue;
            }
           
             
            //if(datetime > dtMin.AddHours(1))
            //{
            //    dtMin = datetime;
            //    tmpPCU = new PcuTimeval(dtMin, onlineNum);
            //    pcuList.Add(tmpPCU);
            //}
            //else
            //{
            //    tmpPCU.m_num += onlineNum;
            //}
        }
        //if(pcuList.Count>0)
        //{
        //    //排序
        //    pcuList.Sort();
        //    //输出最大的时间
        //    int hour = pcuList[pcuList.Count-1].m_pcuTime.Hour;
        //    int nexthour = pcuList[pcuList.Count - 1].m_pcuTime.AddHours(1).Hour;
        //    ret = hour.ToString() + "点-" + nexthour.ToString() + "点";
        //}
        try{
            if (onlineMap.Count > 0)
            {
                var result = from pair in onlineMap orderby pair.Value descending select pair;
            foreach(var  pair in result)
            {
                int maxHour = pair.Key;
                int nextHour = maxHour + 1;
                if (maxHour == 24)
                {
                    nextHour = 0;
                }
                ret = maxHour.ToString()+"点-"+nextHour.ToString()+"点";
                break;
            }
            }
        }
        catch(Exception ex)
        {
            Log.LogError(ex.ToString());
          
        }
       
        return ret;
    }


    //得到一天的用户在线状态表格
    public static bool CreateOnlineTB(DateTime time,ref DataTable tbResult)
    {
        //创建5分钟表格
        string start = time.Year.ToString() + "-" + time.Month.ToString() + "-"
                + time.Day.ToString() + " " + "00:00:00";
        DateTime startTime = Convert.ToDateTime(start);
        DateTime endTime = startTime.AddDays(1);
        for (DateTime timeIndex = startTime; timeIndex<endTime;)
        {
            //创建
            DataColumn colTime = new DataColumn("dtEventTime");
            colTime.DataType = typeof(DateTime);


            timeIndex = timeIndex.AddMinutes(5);
        }
    

        return true;
    }

}
