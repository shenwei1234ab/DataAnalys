﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
using System.IO;

//自定义角色的集合运算
public class PlayerDataRowComparer : IEqualityComparer<DataRow>
{

    public bool Equals(DataRow x, DataRow y)
    {
        // return (x.Field<int>("openid") == y.Field<int>("openid"));  
        string lopenid = x.Field<string>("vopenid");
        string ropenid = y.Field<string>("vopenid");
        UInt32 lSvrid = x.Field<UInt32>("SvrAreaId");
        UInt32 rSvrid = y.Field<UInt32>("SvrAreaId");
        bool ret = false;
        if (lopenid == ropenid && lSvrid == rSvrid)
        {
            ret = true;
        }
        return ret;
    }
    public int GetHashCode(DataRow obj)
    {
        int ret = obj.ToString().GetHashCode();
        return ret;
    }
}


//自定义账号的集合运算
public class AccountDataRowComparer : IEqualityComparer<DataRow>
{

    public bool Equals(DataRow x, DataRow y)
    {
        // return (x.Field<int>("openid") == y.Field<int>("openid"));  
        string lopenid = x.Field<string>("openid");
        string ropenid = y.Field<string>("openid");
        bool ret = false;
        if (lopenid == ropenid )
        {
            ret = true;
        }
        return ret;
    }
    public int GetHashCode(DataRow obj)
    {
        int ret = obj.ToString().GetHashCode();
        return ret;
    }
}

//自定义device的集合运算
public class DeviceDataRowComparer : IEqualityComparer<DataRow>
{

    public bool Equals(DataRow x, DataRow y)
    {
        // return (x.Field<int>("openid") == y.Field<int>("openid"));  
        string ldeviceid = x.Field<string>("deviceid");
        string rdeviceid = y.Field<string>("deviceid");
        bool ret = false;
        if (ldeviceid == rdeviceid)
        {
            ret = true;
        }
        return ret;
    }
    public int GetHashCode(DataRow obj)
    {
        int ret = obj.ToString().GetHashCode();
        return ret;
    }
}
