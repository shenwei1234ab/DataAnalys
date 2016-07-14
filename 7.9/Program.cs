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

namespace ConsoleApplication1
{
      
    class Program
    {

        static void ppidToOpenId()
        {
            long ppid = 46179488366605;
            string openid = Util.PPIdToOpenId(ppid);
            
            Console.Write(openid);
            Console.Write(Util.OpenIdToPPId(openid));
        }

        static string m_channelConfigFile = "channel.txt";
        static string m_diamondPayFile = "DiamondPayRecord.txt";
        static string m_diamondBuyFile = "DiamondBuy.txt";
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
                string msg = "";
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

                //读取区列表id
                DataTable tbResult = new DataTable();
                SqlStament st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_ALL_ZONE);
                if (st == null)
                {
                    Log.LogError("sql:" + st.GetCommand() + "not register");
                    return false;
                }
                if (!st.Execute(ref tbResult, ref msg))
                {
                    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                    return false;
                }
                for (int i = 0; i < tbResult.Rows.Count; ++i)
                {
                    UInt32 SvrAreaId = (UInt32)tbResult.Rows[i]["SvrAreaId"];
                    Svr svr = new Svr(SvrAreaId);
                    m_svrList.Add(svr);
                }

                //读取渠道列表
                st = SqlManager.GetInstance().GetSqlStament(SqlCommand.SELECT_ALL_CHANNEL);
                if (st == null)
                {
                    Log.LogError("sql:" + st.GetCommand() + "not register");
                    return false;
                }
                if (!st.Execute(ref tbResult, ref msg))
                {
                    Log.LogError("sql:" + st.GetCommand() + "execute error" + msg);
                    return false;
                }
                for (int i = 0; i < tbResult.Rows.Count; ++i)
                {
                    Int32 id = (Int32)tbResult.Rows[i]["Channel"];
                    String strChId = id.ToString();
                    if(!m_channelMap.ContainsKey(strChId))
                    {
                        Log.LogDebug("Not found chanel id :" + strChId);
                        continue;
                        // return false;
                    }

                    ////渠道Id不存在
                    //if(m_channelMap[strChId].ChannelId == "?")
                    //{
                    //     continue;
                    //}
                    m_chanList.Add(m_channelMap[strChId]);
                }
                
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
                List<DiamondPresent> presentList = new List<DiamondPresent>();
                presentList = JsonConvert.DeserializeObject<List<DiamondPresent>>(readText);
                foreach (var present in presentList)
                {
                    string id = present.value;
                    string name = present.desc;
                    //第三方不存在渠道ID
                    if (m_diamondPresentMap.ContainsKey(id))
                    {
                        Log.LogError("platId:" + id + "already exist");
                        return false;
                    }
                    m_diamondPresentMap[id] = present;
                }
                 

            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
                return false;
            }
            return true;
        }


      

        static void AddOperationDataTask(DateTime nowTime)
        {
            Log.SetFileName("AddOperationData");
            Log.LogDebug("AddOperationData Start");
            AnalysisSystem system = new AnalysisSystem();
            foreach (var svr in m_svrList)
            {
                //遍历渠道
                foreach (var channl in m_chanList)
                {
                    system.Init(channl, svr);
                    if (!system.AddOperationData(nowTime))
                    {
                        Log.LogError("AddOperationData failed");
                        continue;
                    }
                }
            }
            Log.LogDebug("AddOperationData End");
        }


        static void AddUserRechargesTask(DateTime nowTime,DateTime lastTime)
        {
            Log.SetFileName("AddUserRecharges");
            Log.LogDebug("AddUserRecharges Start");
            AnalysisSystem system = new AnalysisSystem();
            foreach (var svr in m_svrList)
            {
                //遍历渠道
                foreach (var channl in m_chanList)
                {
                    system.Init(channl, svr);
                    //创建订单容器
                    if (!system.AddUserRecharges(nowTime, lastTime))
                    {
                        Log.LogError("AddUserRecharges failed");
                        continue;
                    }
                }
            }
            Log.LogDebug("AddUserRecharges End");
        }


        static void EveryDayTask()
        {
            Log.SetFileName("EveryDay");
            Log.LogDebug("EveryDay start");
            AnalysisSystem system = new AnalysisSystem();
            //遍历区
            foreach(var svr in m_svrList)
            {
                //遍历渠道
                foreach(var channl in m_chanList)
                {
                    system.Init(channl,svr);
                    DateTime time = DateTime.Now.AddDays(-1);
                    if (!system.AddGames())
                    {
                        Log.LogDebug("AddGames failed");
                    }
                    if (!system.AddChannels())
                    {
                        Log.LogDebug("AddChannels failed");
                    }
                    if (!system.AddGamersRetention(time))
                    {
                        Log.LogError("AddGamersRetention failed");
                    }
                    if (!system.AddGamersRoleRetention(time))
                    {
                        Log.LogError("AddGamersRoleRetention failed");
                    }
                    if (!system.AddGamersEquipRetention(time))
                    {
                        Log.LogError("AddGamersEquipRetention failed");
                    }
                    if (!system.AddCommonLtvWorth(time))
                    {
                        Log.LogError("AddCommonLtvWorth failed");
                    }
                    if (!system.AddGamersDayWorth(time))
                    {
                        Log.LogError("AddGamersDayWorth failed");
                    }
                    if (!system.AddEveryDayAddGamerLevelFenbu(time))
                    {
                        Log.LogError("AddEveryDayAddGamerLevelFenbu failed");
                    }
                    if (!system.AddAllGemerLevelFenbu(time))
                    {
                        Log.LogError("AddAllGemerLevelFenbu failed");
                    }
                    if (!system.AddGamerLevelChanges(time))
                    {
                        Log.LogError("AddGamerLevelChanges failed");
                    }
                    if (!system.AddGamerLevelLeft(time))
                    {
                        Log.LogError("AddGamerLevelLeft failed");
                    }
                    if (!system.AddVirtualCornCost(time))
                    {
                        Log.LogError("AddVirtualCornCost failed");
                    }
                    if (!system.AddRechargeGamerInfo(time))
                    {
                        Log.LogError("AddRechargeGamerInfo failed");
                    }
                    }
            }
                        Log.LogDebug("EveryDay End"); 
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
                AddUserRechargesTask(DateTime.Now, m_userRecharlastTime);
                m_userRecharlastTime = DateTime.Now;
            }
            else
            {
                Thread.Sleep(10);
                return;
            }

            if (DateTime.Now.Subtract(m_operationlastTime).TotalSeconds > m_OperationTimeVal)
            {
                AddOperationDataTask(DateTime.Now);
                m_operationlastTime = DateTime.Now;
            }
           

            //每天3：00更新
            if (DateTime.Now > m_everyDay)
            {
                EveryDayTask();
                string day = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-"
            + DateTime.Now.Day.ToString() + " " + m_everyDayTime;
               DateTime dt=  Convert.ToDateTime(day);
               m_everyDay = dt.AddDays(1);
            }

        }
        
        static void Main(string[] args)
        {
            //ppidToOpenId();
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
                return;
            }
            AnalysisSystem system = new AnalysisSystem();
            if (!Init())
            {
                Log.LogError("system Init failed");
                return;
            }

            while(true)
            {
                Update();
            }

             }
        }
    
}
