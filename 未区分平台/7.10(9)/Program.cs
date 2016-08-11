#define _DEBUG
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Newtonsoft.Json.Linq;
using System.IO;
using Newtonsoft.Json;
using System.Threading;
using System.Configuration;

namespace ConsoleApplication1
{
      
    class Program
    {

        //static void ppidToOpenId()
        //{
        //    long ppid = 46179488366605;
        //    string openid = Util.PPIdToOpenId(ppid);
            
        //    Console.Write(openid);
        //    Console.Write(Util.OpenIdToPPId(openid));
        //}

        static void linqTest()
        {

        }

        static string m_channelConfigFile = "channel.txt";
        static string m_svrareaConfigFile = "svrarea.txt";
        static string m_diamondPayFile = "DiamondPayRecord.txt";
        static string m_diamondPresentFile = "DiamondPresentRecord.txt";
        static Dictionary<string, Channel> m_channelMap = new Dictionary<string, Channel>();
        static List<Svr> m_svrList=new List<Svr>();         //区域列表
        static List<Channel> m_chanList = new List<Channel>();//渠道列表
        static Dictionary<string, Channel> GetChanelMap()
        {
            return m_channelMap;
        }

        static Dictionary<string, DiamondPay> m_diamondPayMap= new Dictionary<string, DiamondPay>();


        static Dictionary<string, DiamondPresent> m_diamondPresentMap = new Dictionary<string, DiamondPresent>();

        static bool Init()
        {
#if _DEBUG
            Setting._ifDebug = true;
#else
            Setting._ifDebug = false;
#endif
            try
            {
                //读取开服时间
                 string strStartTime =   ConfigurationManager.AppSettings["StartTime"];
                 DateTime dtStartTime = Convert.ToDateTime(strStartTime);
                //读取渠道列表
                string readText = File.ReadAllText(m_channelConfigFile, System.Text.Encoding.Default);
                //反序列化
                List<Channel> chanList = new List<Channel>();
                chanList = JsonConvert.DeserializeObject<List<Channel>>(readText);
                foreach (var chan in chanList)
                {
                    string id = chan.Id;
                    string channelId = chan.ChannelId;
                    //第三方不存在渠道ID
                    if (m_channelMap.ContainsKey(id))
                    {
                        Log.LogError("platId:" + id + "already exist");
                        return false;
                    }
                    m_channelMap[id] = chan;
                }
                m_chanList = chanList;

                //读取区列表id
                readText = File.ReadAllText(m_svrareaConfigFile, System.Text.Encoding.Default);
                //反序列化
                List<SvrArea> svrList = new List<SvrArea>();
                svrList = JsonConvert.DeserializeObject<List<SvrArea>>(readText);
                foreach (var svrarea in svrList)
                {
                    string id = svrarea.SvrAreaId;
                    Svr svr = new Svr(Convert.ToUInt32(id));
                    m_svrList.Add(svr);
                }

                //DataTable tbResult = new DataTable();
                //SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_ALL_ZONE);
                //if (st == null)
                //{
                //    Log.LogError("sql:" + st.GetCommand() + "not register");
                //    return false;
                //}
                //if (!st.Execute(ref tbResult, ref msg))
                //{
                //    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                //    return false;
                //}
                //for (int i = 0; i < tbResult.Rows.Count; ++i)
                //{
                //    UInt32 SvrAreaId = (UInt32)tbResult.Rows[i]["SvrAreaId"];
                //    Svr svr = new Svr(SvrAreaId);
                //    m_svrList.Add(svr);
                //}

                ////读取渠道列表
                //st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_ALL_CHANNEL);
                //if (st == null)
                //{
                //    Log.LogError("sql:" + st.GetCommand() + "not register");
                //    return false;
                //}
                //if (!st.Execute(ref tbResult, ref msg))
                //{
                //    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                //    return false;
                //}
                //for (int i = 0; i < tbResult.Rows.Count; ++i)
                //{
                //    Int32 id = (Int32)tbResult.Rows[i]["Channel"];
                //    String strChId = id.ToString();
                //    if(!m_channelMap.ContainsKey(strChId))
                //    {
                //        Log.LogDebug("Not found chanel id :" + strChId);
                //        continue;
                //        // return false;
                //    }
                //    m_chanList.Add(m_channelMap[strChId]);
                //}
                
                //读取DiamondPayRecord
                readText = File.ReadAllText(m_diamondPayFile, System.Text.Encoding.Default);
                //反序列化
                 List<DiamondPay> payList = new List<DiamondPay>();
                 payList = JsonConvert.DeserializeObject<List<DiamondPay>>(readText);
                 foreach (var pay in payList)
                 {
                     string id = pay.value;
                     string name = pay.desc;
                     //第三方不存在渠道ID
                     if (m_diamondPayMap.ContainsKey(id))
                     {
                         Log.LogError("platId:" + id + "already exist");
                         return false;
                     }
                     m_diamondPayMap[id] = pay;
                 }
                //读取DiamondPresentRecord
                 readText = File.ReadAllText(m_diamondPresentFile, System.Text.Encoding.Default);
                List<DiamondPresent> presentList = new List<DiamondPresent>();
                presentList = JsonConvert.DeserializeObject<List<DiamondPresent>>(readText);
                foreach (var present in presentList)
                {
                    string id = present.value;
                    string name = present.desc;
                    if (m_diamondPresentMap.ContainsKey(id))
                    {
                        Log.LogError("platId:" + id + "already exist");
                        return false;
                    }
                    m_diamondPresentMap[id] = present;
                }
                //初始化
               if( !m_userRechargeTask.Init(m_svrList, m_chanList))
               {
                   return false;
               }

               if (!m_everydayTask.Init(m_svrList, m_chanList, m_diamondPayMap, m_diamondPresentMap, dtStartTime))
               {
                   return false;
               }
                if(!m_operationTask.Init(m_svrList,m_chanList))
                {
                    return false;
                }


               return true;
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
                return false;
            }
        }


     

        static AddUserRechargesTask m_userRechargeTask = new AddUserRechargesTask();
        static AddOperationDataTask m_operationTask = new AddOperationDataTask();
        static AnalysisSystem m_everydayTask = new AnalysisSystem();

        static void AddUserRechargesTask(DateTime nowTime,DateTime lastTime)
        {
            m_userRechargeTask.RunTask(nowTime, lastTime);
        }


        static void EveryDayTask()
        {
            DateTime time = DateTime.Now.AddDays(-1);
            m_everydayTask.RunTask(time);
        }
        static void AddOperationDataTask(DateTime nowTime)
        {
            m_operationTask.RunTask(nowTime);
        }
     

        static double m_OperationTimeVal = 10*60;        //10分钟
        static double m_UserRechargeTimeVal = 1 * 60 ;  //充值1分钟
        static DateTime m_operationlastTime=DateTime.Now;
        static DateTime m_userRecharlastTime = DateTime.Now;
        static DateTime m_everyDay;
        static string m_everyDayTime = "3:00:00";



        static void Update()
        {
            if (DateTime.Now.Subtract(m_userRecharlastTime).TotalSeconds > m_UserRechargeTimeVal)
            {
                DateTime nowTime = DateTime.Now;
                try
                { 
                    m_userRechargeTask.RunTask(nowTime, m_userRecharlastTime);
                }
                catch(Exception ex)
                {
                    Log.SetFileName("Fatal ERROR");
                    Log.LogError("AddUserRechargesTask fatal error"+ex.ToString());
                }
                m_userRecharlastTime = nowTime;
            }
            else
            {
                Thread.Sleep(10);
                return;
            }

            if (DateTime.Now.Subtract(m_operationlastTime).TotalSeconds > m_OperationTimeVal)
            {
                DateTime now = DateTime.Now;
                try
                {
                    m_operationTask.RunTask(now);
                }
                catch (Exception ex)
                {
                    Log.SetFileName("Fatal ERROR");
                    Log.LogError("AddOperationDataTask fatal error" + ex.ToString());
                }
                m_operationlastTime = now;
            }
           

            //每天3：00更新
            if (DateTime.Now > m_everyDay)
            {
                try
                {
                    DateTime time = DateTime.Now.AddDays(-1);
                    m_everydayTask.RunTask(time);
                }
                catch(Exception ex)
                {
                    Log.SetFileName("Fatal ERROR");
                    Log.LogError("EveryDayTask fatal error" + ex.ToString());
                }
                string day = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-"
            + DateTime.Now.Day.ToString() + " " + m_everyDayTime;
               DateTime dt=  Convert.ToDateTime(day);
               m_everyDay = dt.AddDays(1);
            }

        }
        


        /// <summary>
        /// 测试用例 
        /// </summary>
        /// <param name="args"></param>
        static void MyEveryDayTest(DateTime dtStart, DateTime dtEnd)
        {
            //收费测试
            //DateTime lastTime = dtStart;
            //DateTime FinTime = dtEnd;
            //DateTime nowTime = lastTime.AddMinutes(1);
            //while (nowTime <= FinTime)
            //{
            //    AddUserRechargesTask(nowTime, lastTime);
            //    lastTime = nowTime;
            //    nowTime = nowTime.AddMinutes(1);
            //}

            DateTime fiveStart = dtStart;
            for (; fiveStart <= dtEnd; )
            {
                m_operationTask.RunTask(fiveStart);
                fiveStart = fiveStart.AddDays(1);
            }

            ////每天任务测试
            //DateTime everyStart = dtStart;
            //for (; everyStart <= dtEnd; )
            //{
            //    m_everydayTask.RunTask(everyStart);
            //    everyStart = everyStart.AddDays(1);
            //}
        }



     
        static void Main(string[] args)
        {
            string day = DateTime.Now.Year.ToString()+"-"+ DateTime.Now.Month.ToString()+"-"
            + DateTime.Now.Day.ToString() + " " + m_everyDayTime;
            m_everyDay = Convert.ToDateTime(day);
            if (!SqlManager.GetInstance().Init())
            {
                Log.LogError("sqlManager init failed");
                return;
            }
            if(!Log.Init())
            {
                Log.LogError("LoadLogConfig failed");
                //return;
            }
            if (!Init())
            {
                Log.LogError("system Init failed");
                return;
            }

            //test
            DateTime dtStart = Convert.ToDateTime("2016-08-11 11:30:00");
            DateTime dtEnd = Convert.ToDateTime("2016-08-11 17:40:00");
            MyEveryDayTest(dtStart, dtEnd);
            //while (true)
            //{
            //    Update();
            //}
        }
    }
    
}
