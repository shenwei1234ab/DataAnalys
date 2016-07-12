using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Newtonsoft.Json.Linq;
using System.IO;
using Newtonsoft.Json;
namespace ConsoleApplication1
{
    class Program
    {
        //static void ppidtest()
        //{
        //    //netlog的vopenid （ varchar(64)）
        //    string vopenid = "dBAAAAAcAAA=";

        //    //tdgame 的ppid bigint(64) unsigned
        //    string ppid = Util.OpenIdToPPId(vopenid);
                
        //    Console.Write(ppid);
        //}

        static void ppidToOpenId()
        {
            long ppid = 1099511627780;
            string openid = Util.PPIdToOpenId(ppid);
            
            Console.Write(openid);
            Console.Write(Util.OpenIdToPPId(openid));
        }

        static string m_channelConfigFile = "channel.txt";
        static Dictionary<string, Channel> m_channelMap = new Dictionary<string, Channel>();
        static List<Svr> m_svrList=new List<Svr>();         //区域列表
        static List<Channel> m_chanList = new List<Channel>();//渠道列表
        static Dictionary<string, Channel> GetChanelMap()
        {
            return m_channelMap;
        }
        static bool Init()
        {       
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
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
                return false;
            }
            return true;
        }


        static void AddOperationDataTask(object o)
        {

        }


        static void 
        static void Main(string[] args)
        {

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


            //定义AddOperationData定时器
            System.Threading.Timer timerOperation = new System.Threading.Timer(
               new System.Threading.TimerCallback(Tick), null, 0, minutes * 60 * 1000);
            GC.KeepAlive(timerOperation);






            //
                switch (args[0])
                {

                    case "EveryDay":
                        {
                            Log.SetFileName("EveryDay");
                            Log.LogDebug("EveryDay start");
                            //遍历区
                            foreach(var svr in m_svrList)
                            {
                                //遍历渠道
                                foreach(var channl in m_chanList)
                                {
                                    system.Init(channl,svr);
                                    DateTime time = DateTime.Now.AddDays(-1);
                                    //test
                                   // time = DateTime.Now.AddDays(0);
                                    //time = Convert.ToDateTime("2016-07-08 19:29:20"); 
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
                        break;
                        //运营数据统计10分钟
                    case "AddOperationData":
                        {
                            Log.SetFileName("AddOperationData");
                            Log.LogDebug("AddOperationData Start");
                            foreach (var svr in m_svrList)
                            {
                                //遍历渠道
                                foreach (var channl in m_chanList)
                                {
                                    system.Init(channl, svr);
                                    //
                                    DateTime time = DateTime.Now.AddDays(0);
                                    //DateTime time = Convert.ToDateTime("2016-07-06 19:29:20");
                                    if (!system.AddOperationData(time))
                                    {
                                        Log.LogError("AddOperationData failed");
                                        continue;
                                    }
                                }
                            }
                            Log.LogDebug("AddOperationData End");
                        }
                        break;
                        //个人充值1分钟
                    case "AddUserRecharges":
                        {
                            Log.SetFileName("AddUserRecharges");
                            Log.LogDebug("AddUserRecharges Start");
                            foreach (var svr in m_svrList)
                            {
                                //遍历渠道
                                foreach (var channl in m_chanList)
                                {
                                    system.Init(channl, svr);
                                    DateTime time = DateTime.Now.AddDays(0);
                                    if (!system.AddUserRecharges(time))
                                    {
                                        Log.LogError("AddUserRecharges failed");
                                        continue;
                                    }
                                }
                            }
                            Log.LogDebug("AddUserRecharges End");
                        }
                        break;
                    default:
                        Log.LogError("Invalid Parmas");
                        break;
                }
             }
        }
    
}
