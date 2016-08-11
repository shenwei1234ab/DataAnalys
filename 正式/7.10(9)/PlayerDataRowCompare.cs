using System;
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


//
public class PayAccountDataRowComparer : IEqualityComparer<DataRow>
{

    public bool Equals(DataRow x, DataRow y)
    {
        // return (x.Field<int>("openid") == y.Field<int>("openid"));  
        UInt64 lppid = x.Field<UInt64>("ppid");
        UInt64 rppid = y.Field<UInt64>("ppid");
        bool ret = false;
        if (lppid == rppid )
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