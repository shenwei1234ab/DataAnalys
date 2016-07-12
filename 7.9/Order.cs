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

public enum rechargetState
{
    NoPayed = 0,
    Payed
}

public class Order
{
    public Order(string orderid, string ppid, string userNickname,string playerid,
        string accountCreateTime, string orderCreateTime, string orderMoney, string channelOrderNo)
    {
        m_orderid = orderid;
        m_ppid = ppid;
        m_userNickname = userNickname;
        m_playerid = playerid;
       
        m_accountCreateTime = accountCreateTime;
        m_orderCreateTime = orderCreateTime;
        m_money = orderMoney;
        m_channelOrderNo = channelOrderNo;
    
    }
    public string m_playerid = "";
    public rechargetState m_rechargeState;
    public string m_orderid="";
    public string m_ppid = "";
     public string m_userId="";
     public string m_userNickname="";
     public string m_money="";
     public OrderState m_orderState;
     public string m_orderCreateTime;
     public string m_accountCreateTime;
     public string m_channelOrderNo="";
}
