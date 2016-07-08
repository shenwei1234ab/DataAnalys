using System;
//区



public enum OrderState
{
    NoData = 0,
    CreateFailed ,
    CreateSuccess,
    PaySuccess,
    PayFailed,
    NotifySuccess,
    NotifyFailed,
    Other
}


public class Order
{
    public Order(string orderid,string userId, string userNickname, string money, DateTime rechargeTime)
    {
        m_orderid = orderid;
        m_userId = userId;
        m_userNickname = userNickname;
        m_money = money;
        m_rechargeTime = rechargeTime;
    }
    public string m_orderid;
     public string m_userId;
     public string m_userNickname;
     public string m_money;
     public OrderState m_orderState;
     public DateTime m_rechargeTime;
}
