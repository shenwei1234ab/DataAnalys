using System;
//区
public class Svr
{
    public Svr(UInt32 SvrId)
    {
        m_SvrAreaId = SvrId;
        m_SvrAreaName = Util.GetIPString(SvrId);
    }
    public UInt32 m_SvrAreaId;      //区id
    public string m_SvrAreaName;    //服务器ip:127.0.0.1(2)
}
