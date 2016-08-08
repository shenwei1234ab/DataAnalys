using System;
//区
public class Platform
{
    public Platform(int platId)
    {
        m_platformId = platId;
       if(platId == 0)
       {
           m_platformName = "ios";
       }
       else
       {
           m_platformName = "android";
       }
    }
    public int m_platformId;      //平台id(android ios)
    public string m_platformName;    //平台名称：（android ios）
}
