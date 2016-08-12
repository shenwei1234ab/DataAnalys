using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
//批量接口
public class GamerLevelLeftInfo
{
    public string leDateTime{ get; set; }  
    public string gameServerIp{ get; set; }  
    public Int64 leGamerLevel{ get; set; }  
    public int leGamerNumber{ get; set; }
    public string platform { get; set; }
}



public class VirtualCornCostInfo
{
    public string coDateTime { get; set; }
    public string gameServerIp { get; set; }
    public string coCostCategoryName { get; set; }
    public int coCostNumber { get; set; }
    public string platform { get; set; }
}


public class GamerLevelChangeInfo
{
    public string chDateTime{ get; set; }
    public string gameServerIp { get; set; }
    public Int64 chGamerLevel { get; set; }
    public int chGamerNumber{ get; set; }
    public string platform { get; set; }
}



public class AllGamerLevelFenbuInfo
{
    public string allDateTime { get; set; }
    public string gameServerIp { get; set; }
    public Int64 allGamerLevel { get; set; }
    public int allGamerNumber { get; set; }
    public string platform { get; set; }
}



public class EverydayAddGamerLevelFenbuInfo
{
    public string evDateTime { get; set; }
    public string gameServerIp { get; set; }
    public Int64 evGamerLevel { get; set; }
    public int evGamerNumber { get; set; }
    public string platform { get; set; }
}



public class BigRechargeGamerInfo
{
    public string caculateDateTime { get; set; }
    public string inRoleName { get; set; }
    public string gameServerIp { get; set; }

    public string inRoleId { get; set; }

    public string inEquip { get; set; }

    public string inRechargeMoney { get; set; }
    public string inRechargeLevel { get; set; }


    public string inDateTime { get; set; }

    public UInt32 inVirtualCornOwnNumber { get; set; }

    public string inVirtualCornSumCost { get; set; }


    public string platform { get; set; }
}