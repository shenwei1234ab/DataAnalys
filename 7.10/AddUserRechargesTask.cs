using System;
using System.Collections.Generic;
using System.Data;
//区



public class AddUserRechargesTask:Task
{


    //读取渠道区，列表
  

    //public  void RunTask(DateTime nowTime, DateTime lastTime)
    //{
    //    Log.SetFileName("AddUserRecharges");
    //    Log.LogDebug("AddUserRecharges Start");
    //    AnalysisSystem system = new AnalysisSystem();
    //    foreach (var svr in m_svrList)
    //    {
    //        //遍历渠道
    //        foreach (var channl in m_chanList)
    //        {
    //            system.Init(channl, svr);
    //            //创建订单容器
    //            if (!system.AddUserRecharges(nowTime, lastTime))
    //            {
    //                Log.LogError("AddUserRecharges failed");
    //                continue;
    //            }
    //        }
    //    }
    //    Log.LogDebug("AddUserRecharges End");
    //}




    public bool AddUserRecharges(DateTime date, DateTime lastTime)
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
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?lastdate", lastTime.ToString()));
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
                if (result < 0)
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
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
                continue;
            }
        }
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
"&platform=" + m_platform +
"&gamerServerName=" + m_svr.m_SvrAreaId +
"&accountCreateTime=" + order.m_accountCreateTime +
"&gamerPhone=" + "" +
"&orderCreateTime=" + order.m_orderCreateTime +
"&orderMoney=" + order.m_money +
"&rechargeMoney=" + order.m_money +
"&rechargeFunc=" + "" +
 "&rechargetState=" + (int)order.m_rechargeState +
 "&orderState=" + (int)order.m_orderState;
            if (Post(ref strMsg) == 0)
            {
                Log.LogError("AddUserRecharges failed:" + strMsg);
                ret = false;
            }
        }
        return ret;
    }

}
