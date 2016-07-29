using System;
using System.Collections.Generic;
using System.Data;
//区



public class AddUserRechargesTask:Task
{
    public void RunTask(DateTime nowTime, DateTime lastTime)
    {
        Log.SetFileName("AddUserRecharges");
        Log.LogDebug("AddUserRecharges Start");
        foreach (var TaskData in m_taskData)
        {
            AddUserRecharges(nowTime, lastTime, TaskData);
        }

        Log.LogDebug("AddUserRecharges End");
    }
 

    private bool AddUserRecharges(DateTime date, DateTime lastTime,TaskData data)
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
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?Channel", data.m_channel.Id));
        st.SetParameter(new MySql.Data.MySqlClient.MySqlParameter("?SvrAreaId", data.m_svr.m_SvrAreaId));
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

        //下单成功的(status=0)订单,下个一分钟要处理
        Dictionary<string, Order> newOrderMap = new Dictionary<string,Order>();
        //处理当前一分钟的订单
        for (int i = 0; i < resultTb.Rows.Count; ++i)
        {
            try
            {
                string orderid = resultTb.Rows[i]["orderid"].ToString();
                string ppid = resultTb.Rows[i]["ppid"].ToString();
                string nickname = resultTb.Rows[i]["nickname"].ToString();
                string playerid = resultTb.Rows[i]["playerid"].ToString();
                string accountCreateTime = resultTb.Rows[i]["accountCreateTime"].ToString();
                DateTime orderCreateTime = (DateTime)resultTb.Rows[i]["orderCreateTime"];
                string orderMoney = resultTb.Rows[i]["price"].ToString();
                // string rechargeMoney = resultTb.Rows[i]["price"].ToString();
                string status = resultTb.Rows[i]["status"].ToString();
                //查询渠道订单号
                string channelOrderNo = "";
                if(resultTb.Rows[i]["pforderid"]!=null)
                {
                    channelOrderNo = resultTb.Rows[i]["pforderid"].ToString();
                }
                //      
                string mainChannelNamel = "";
                if (resultTb.Rows[i]["platformid"] != null )
                {
                   if(resultTb.Rows[i]["platformid"].ToString() == "42")
                   {
                       mainChannelNamel = "棱镜";
                   }
                }
                //查询上一分钟的下单成功的订单
                if (data.m_orderDatas.ContainsKey(orderid))
                {
                    if(status == "2")
                    {
                        //移除
                        data.m_orderDatas.Remove(orderid);
                    }
                }
          
                if (status == "2")
                {
                    Order successOrder = new Order(orderid, ppid, nickname, playerid, accountCreateTime, orderCreateTime.ToString(), decimal.Parse(orderMoney).ToString("#0.00"), channelOrderNo, mainChannelNamel);
                    successOrder.m_orderState = OrderState.PaySuccess;
                    successOrder.m_rechargeState = rechargetState.Payed;
                    orderList.Add(successOrder);

                    Order order = new Order(orderid, ppid, nickname, playerid, accountCreateTime, orderCreateTime.ToString(), decimal.Parse(orderMoney).ToString("#0.00"), channelOrderNo, mainChannelNamel);
                    order.m_orderState = OrderState.NotifySuccess;
                    order.m_rechargeState = rechargetState.Payed;
                    orderList.Add(order);
                }
                else if (status == "0")
                {
                    //TimeSpan span = (TimeSpan)(date - orderCreateTime);
                    ////todo
                    //if (span.Minutes < 1)
                    //{
                    //    order.m_orderState = OrderState.CreateSuccess;
                    //}
                    //else
                    //{
                    //    order.m_orderState = OrderState.PayFailed;
                    //}
                    //order.m_rechargeState = rechargetState.NoPayed;
                    
                    Order order = new Order(orderid, ppid, nickname, playerid, accountCreateTime, orderCreateTime.ToString(), decimal.Parse(orderMoney).ToString("#0.00"), channelOrderNo, mainChannelNamel);
                    order.m_orderState = OrderState.CreateSuccess;
                    order.m_rechargeState = rechargetState.NoPayed;
                    orderList.Add(order);
                    if (newOrderMap.ContainsKey(orderid))
                    {
                        Log.LogError("duplicat orderid"+orderid);
                        continue;
                    }
                    newOrderMap.Add(orderid,order);
                }
                else
                {
                    Log.LogError("UNknown orderid:" + orderid + "orderstatus:" + status);
                       continue;
                }
            }
            catch (Exception ex)
            {
                Log.LogError(ex.ToString());
                continue;
            }
        }


        //处理上一分钟状态为0的订单本分钟还没有完成判定为支付失败PayFailed
        foreach (var iter in data.m_orderDatas)
        {
            Order failOrder = iter.Value;
            failOrder.m_orderState= OrderState.PayFailed;
            orderList.Add(failOrder);
        }
        data.m_orderDatas = newOrderMap;
        
        //提交订单
        foreach (var order in orderList)
        {
            m_postData =
      "gameOrderNo=" + order.m_orderid.ToString() +
       "&channelOrderNo=" + order.m_channelOrderNo.ToString() +
     "&gameServerIp=" + data.m_postSvrInfo +
 "&userToken=" + m_userToken +
  "&gamerAccount=" + order.m_ppid +
  "&roleName=" + order.m_userNickname +
  "&roleId=" + order.m_playerid +
  "&mainChannelName=" + order.m_mainChannelName +
"&equipId=" + "" +
"&platform=" + m_platform +
"&gamerServerName=" + data.m_svr.m_SvrAreaId +
"&accountCreateTime=" + order.m_accountCreateTime +
"&gamerPhone=" + "" +
"&orderCreateTime=" + order.m_orderCreateTime +
"&orderMoney=" + order.m_money +
"&rechargeMoney=" + order.m_money +
"&rechargeFunc=" + "" +
 "&rechargetState=" + (int)order.m_rechargeState +
 "&orderState=" + (int)order.m_orderState;
            if (Post(data,ref strMsg) == 0)
            {
                Log.LogError("AddUserRecharges failed:" + strMsg);
                ret = false;
            }
        }
        return ret;
    }

}
