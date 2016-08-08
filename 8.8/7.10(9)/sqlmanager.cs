using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Text.RegularExpressions;

public enum SqlCommand
{




    SELECT_ALL_ZONE,
    SELECT_ALL_CHANNEL,


    
    SELECT_COUNT_REGAccount,
    SELECT_COUNT_REGPLAYER,
    SELECT_COUNT_REGDEVICE,



    SELECT_COUNT_LOGINAccount,                //查询某天的登录人数
    SELECT_LOGINAccount,

    SELECT_COUNT_LOGINPLAYER,
    SELECT_LOGINPLAYER,
    SELECT_COUNT_LOGINDEVICE,




    SELECT_COUNT_NEWAccount,
    SELECT_NEWAccount,
    SELECT_COUNT_NEWPLAYER,
    SELECT_NEWPLAYER,
    SELECT_COUNT_NEWDEVICE, 




    SELECT_ACU,                         //平均同时在线人数
    SELECT_PCU,                         //最高在线人数
    SELECT_PCU_TIMEINTERVAL,            //pcu时段
    SELECT_ALL_ONLINECNT,           //所有在线区段
    SELECT_SUM_ONLINETIME,



    //select online status
    SELECT_ONLINESTATUS_ACCOUNT,





    SELECT_STILL_LOGINACCOUNT,
    SELECT_STILL_LOGINPLAYER,
    SELECT_STILL_LOGINDEVICE,


    SELECT_NEWRPLAYER_LEVEL,  
    SELECT_PLAYER_LEVEL,            //指定玩家的等级信息
    SELECT_ALLPLAYER_LEVEL,
    SELECT_ALLPLAYER_LEVELUP,
    SELECT_LOGOUTPLAYER_LEVEL,
           

    //钻石数据
    SELECT_DIAMONDPAY,
    SELECT_DIAMONDPRESENT,



    //鲸鱼数据
    SELECT_RECHARGEPLAYER_DEVICEID,
    SELECT_RECHARGEPLAYER_LEVEL,
    SELECT_RECHARGEPLAYER_DIAMOND_CONSUMCOST,
    MAX_DB_NETLOG,






 
    SELECT_PAY_ACCOUNT,                 //今日付费账号
    SELECT_PAY_ACCOUNT_Before,
    SELECT_PAY_ACCOUNTBY_PPID,                                    //某个账号今日的付费
    SELECT_PAY_Player,
    SELECT_PAY_Player_Before,

    SELECT_RechargeGamerInfo,                                 //鲸鱼数据
    SELECT_AddUserRecharges,            //个人充值
    SELECT_AddUserRecharges_ChannelOrderNo,         //查询渠道订单号

    SELECT_Test,

    SELECT_PAY_ACCOUNT_COUNT,            //账号付费次数



    SELECT_FIRSTPAY_ACCOUNT,              //新登付费账号
    SELECT_FIRSTPAY_PLAYER,
    SELECT_FIRSTPAY_DEVICE,



    SELECT_LTV_ACCOUNT_PAY,
    SELECT_ACCOUNT_PAY,
    MAX_TDGAME_46001,
}

class SqlManager
{
    private static SqlManager m_instance=null;
    private SqlManager()
    {

    }

    public static SqlManager GetInstance()
    {
        if(m_instance == null)
        {
            m_instance = new SqlManager();
        }
        return m_instance;
    }

    /// <summary>
    /// 注册sql 语句 ,todo 检查sql 语句的合法性
    /// </summary>
    public bool Init()
   {
        //读取配置文件获取连接字符串
       if (!FileSystem.LoadDBConfig(ref m_netlogCon, ref m_tdgameCon))
       {
           Log.LogError("LoadDBConfig failed");
           return false;
       }



       //date1 <x<=date2在线玩家=  date1 <dtLogout<=date2  +   date1<dtLogin<=date2 && dtlogout>=date2
       RegisterSqlCommand((int)SqlCommand.SELECT_ONLINESTATUS_ACCOUNT,
        @"select vopenid
        from  OnlinePlayer 
        where dtEventTime>?date1 
        and dtEventTime<=?date2
        and Channel=?Channel 
        and SvrAreaId=?SvrAreaId");
        






        //获取注册玩家的所有区id
       RegisterSqlCommand((int)SqlCommand.SELECT_ALL_ZONE,
   @"select SvrAreaId 
        from  PlayerRegister 
        GROUP BY SvrAreaId");



       RegisterSqlCommand((int)SqlCommand.SELECT_ALL_ONLINECNT,
  @"select dtEventTime,OnlineCnt 
        from  OnlinePlayer
         where date(dtEventTime)=date(?date) 
        and Channel=?Channel 
        and SvrAreaId=?SvrAreaId");



       RegisterSqlCommand((int)SqlCommand.SELECT_ALL_CHANNEL,
  @"select Channel 
        from  PlayerRegister 
        GROUP BY Channel");


        //test
       RegisterSqlCommand((int)SqlCommand.SELECT_Test,
    @"SELECT order_data.orderid,account.ppid,player.diamond, player.playerid,account.nickname,product.price ,order_data.timestamp  
        FROM order_data,account,player,product 
        WHERE order_data.playerid = player.playerid 
        AND order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid 
        AND date(order_data.`timestamp`) >= date(?date) 
        AND date(order_data.`timestamp`) <= date(?date)
        AND account.platformid=?platformid");


     
       //某日注册账号数
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_REGAccount, 
        @"select count(DISTINCT vopenid ) as count 
        from  PlayerRegister 
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel
        and SvrAreaId=?SvrAreaId");
      

        //某日注册角色数
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_REGPLAYER,
        @"select count( vopenid ) as count 
        from  PlayerRegister 
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel
        and SvrAreaId=?SvrAreaId");


        //某日注册设备数
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_REGDEVICE,
        @"select count(DISTINCT DeviceId) as count 
        from  PlayerRegister
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel 
        and SvrAreaId=?SvrAreaId");



        //某日登陆账号数
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_LOGINAccount,
        @"select count(DISTINCT vopenid ) as count 
        from  PlayerLogin 
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel
        and SvrAreaId=?SvrAreaId");


       //某日登陆账号
       RegisterSqlCommand((int)SqlCommand.SELECT_LOGINAccount, 
        @"select DISTINCT vopenid  as openid  
        from  PlayerLogin
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel 
        and SvrAreaId=?SvrAreaId");



       //某日登陆角色数
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_LOGINPLAYER,
        @"select count(DISTINCT vopenid ,SvrAreaId) as count 
        from  PlayerLogin 
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel 
        and SvrAreaId=?SvrAreaId ");


        RegisterSqlCommand((int)SqlCommand.SELECT_LOGINPLAYER,
        @"select  vopenid ,SvrAreaId,Max(dtEventTime) as dtEventTime
        from  PlayerLogin 
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel 
        and SvrAreaId=?SvrAreaId 
        group by vopenid,SvrAreaId");


       //某日登陆设备数
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_LOGINDEVICE,
        @"select count(DISTINCT DeviceId) as count
        from  PlayerLogin 
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel 
        and SvrAreaId=?SvrAreaId");




        //某日新登账号数=某日注册账号-DeviceId已经注册过的
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_NEWAccount,
        @"select count(DISTINCT vopenid)as count  
        from  PlayerRegister 
        where date(dtEventTime)=date(?date) and Channel=?Channel and SvrAreaId=?SvrAreaId
        and DeviceId not in
        (select DISTINCT DeviceId from PlayerRegister where date(dtEventTime)<date(?date) and Channel=?Channel and SvrAreaId=?SvrAreaId)");


        //某日新登账号
       RegisterSqlCommand((int)SqlCommand.SELECT_NEWAccount,
        @"select DISTINCT vopenid as openid 
        from  PlayerRegister 
        where date(dtEventTime)=date(?date) and Channel=?Channel and SvrAreaId=?SvrAreaId
        and DeviceId not in
        (select DISTINCT DeviceId from PlayerRegister where date(dtEventTime)<date(?date) and Channel=?Channel and SvrAreaId=?SvrAreaId)");




       //某日新登角色数（某日注册角色-DeviceId已经注册过的）
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_NEWPLAYER,
        @"select count(DISTINCT vopenid,SvrAreaId)as count  
        from  PlayerRegister 
        where date(dtEventTime)=date(?date)
        and Channel=?Channel  
        and SvrAreaId=?SvrAreaId
        and DeviceId not in
        (select DISTINCT DeviceId from PlayerRegister where date(dtEventTime)<date(?date) and Channel=?Channel and SvrAreaId=?SvrAreaId)");


       RegisterSqlCommand((int)SqlCommand.SELECT_NEWPLAYER,
      @"select DISTINCT vopenid,SvrAreaId
        from  PlayerRegister 
        where date(dtEventTime)=date(?date)
        and Channel=?Channel  
        and SvrAreaId=?SvrAreaId
        and DeviceId not in
        (select DISTINCT DeviceId from PlayerRegister where date(dtEventTime)<date(?date) and Channel=?Channel and SvrAreaId=?SvrAreaId)");



       //某日新登设备 = 某日的设备 - DeviceId已经注册过的
       RegisterSqlCommand((int)SqlCommand.SELECT_COUNT_NEWDEVICE,
        @"select count(DISTINCT DeviceId )as count 
        from  PlayerRegister 
        where date(dtEventTime)=date(?date)
        and Channel=?Channel
        and SvrAreaId=?SvrAreaId
        and DeviceId not in
        (select DISTINCT DeviceId from PlayerRegister where date(dtEventTime)<date(?date) and Channel=?Channel  and SvrAreaId=?SvrAreaId)");


        //date新登账号在之后datelogin日依然登陆（内连接）的账号个数
       RegisterSqlCommand((int)SqlCommand.SELECT_STILL_LOGINACCOUNT,
        @"SELECT count(*) as count 
        FROM 
        (select DISTINCT vopenid  as openid from  PlayerRegister where date(dtEventTime)=date(?date)  and Channel=?Channel and SvrAreaId=?SvrAreaId and DeviceId not in(select DISTINCT DeviceId from PlayerRegister where date(dtEventTime)<date(?date) and Channel=?Channel and SvrAreaId=?SvrAreaId))t1,
        (select DISTINCT vopenid  as openid from  PlayerLogin where date(dtEventTime)=date(?datelogin) and Channel=?Channel and SvrAreaId=?SvrAreaId)t2
        where t1.openid = t2.openid");


        //date新登角色在之后datelogin日依然登陆（内连接）的个数
       RegisterSqlCommand((int)SqlCommand.SELECT_STILL_LOGINPLAYER,
        @"SELECT count(*) as count 
        FROM 
        (select DISTINCT vopenid as openid,SvrAreaId from    PlayerRegister where date(dtEventTime)=date(?date) and Channel=?Channel  
          and SvrAreaId=?SvrAreaId
          and DeviceId not in
        (select DISTINCT DeviceId from PlayerRegister where date(dtEventTime)<date(?date) and Channel=?Channel 
        and SvrAreaId=?SvrAreaId))t1,
        (select DISTINCT vopenid  as openid,SvrAreaId from  PlayerLogin where date(dtEventTime)=date(?datelogin) and Channel=?Channel and SvrAreaId=?SvrAreaId)t2 
        where t1.openid = t2.openid and t1.SvrAreaId = t2.SvrAreaId");


        //datereg新登机器之后datelogin日依然登陆（内连接）的个数
       RegisterSqlCommand((int)SqlCommand.SELECT_STILL_LOGINDEVICE,
       @"SELECT count(*) as count 
        FROM 
        ( select  DISTINCT DeviceId  from  PlayerRegister where date(dtEventTime)=date(?date) 
         and Channel=?Channel
       and SvrAreaId=?SvrAreaId
        and DeviceId not in
        (select DISTINCT DeviceId from PlayerRegister where date(dtEventTime)<date(?date)  and Channel=?Channel  and SvrAreaId=?SvrAreaId ))t1,
        (select DISTINCT DeviceId  from  PlayerLogin where date(dtEventTime)=date(?datelogin) and Channel=?Channel   and SvrAreaId=?SvrAreaId)t2
        where t1.DeviceId = t2.DeviceId ");



       RegisterSqlCommand((int)SqlCommand.SELECT_ACU,
        @"select ifnull(AVG(OnlineCnt),0) as acu 
        from OnlinePlayer 
        where date(dtEventTime)=date(?date) 
        and Channel=?Channel
        and SvrAreaId=?SvrAreaId");

       RegisterSqlCommand((int)SqlCommand.SELECT_PCU,
        @"select ifnull(MAX(OnlineCnt),0) as pcu 
        from OnlinePlayer 
        where date(dtEventTime)=date(?date)
         and Channel=?Channel
        and SvrAreaId=?SvrAreaId");



       RegisterSqlCommand((int)SqlCommand.SELECT_PCU_TIMEINTERVAL, "select dtEventTime as timeval from OnlinePlayer where OnlineCnt=(?pcu) and date(dtEventTime)=date(?date) and Channel=?Channel and SvrAreaId=?SvrAreaId order by dtEventTime asc limit 1");

       //某日总在线时SELECT_PAY_Player长
       RegisterSqlCommand((int)SqlCommand.SELECT_SUM_ONLINETIME,
        @"select ifnull(sum(iOnlineTime),0) as count
        from PlayerLogout
        where date(dtEventTime)=date(?date) 
        AND Channel=?Channel
        and SvrAreaId=?SvrAreaId");









       //某日新增角色的等级分布 =      当日升级角色  && 当日某渠道新增角色   （右外连接）
       RegisterSqlCommand((int)SqlCommand.SELECT_NEWRPLAYER_LEVEL,
           @"select ifnull(t1.MaxLevel,1)as level,t2.vopenid,t2.SvrAreaId
from 
(select vopenid,SvrAreaId,MAX(AfterLevel)as MaxLevel from PlayerLvUpFlow 
where date(dtEventTime)=date(?date)
 and SvrAreaId=?SvrAreaId
group by vopenid ,SvrAreaId)t1 

right outer join 
(select DISTINCT vopenid,SvrAreaId,Channel
        from  PlayerRegister 
        where date(dtEventTime)=date(?date)
        and Channel=?Channel  
        and SvrAreaId=?SvrAreaId
        and DeviceId not in
        (select DISTINCT DeviceId from PlayerRegister 
        where date(dtEventTime)<date(?date) ))t2 

on t1.vopenid=t2.vopenid and t1.SvrAreaId = t2.SvrAreaId");




       //SELECT_PLAYER_LEVEL
       //角色等级  
       RegisterSqlCommand((int)SqlCommand.SELECT_PLAYER_LEVEL,
    @"select MAX(AfterLevel)as level, vopenid,SvrAreaId 
    from PlayerLvUpFlow 
    where date(dtEventTime)<=date(?date)
    and  vopenid=?vopenid
    and SvrAreaId=?SvrAreaId 
    group by vopenid ,SvrAreaId  
    ");


        //某日所有角色的等级分布 = 注册过的所有角色的等级
       RegisterSqlCommand((int)SqlCommand.SELECT_ALLPLAYER_LEVEL,
    @"select ifnull(t1.MaxLevel,1)as level,t2.vopenid,t2.SvrAreaId 
from 
(select  vopenid,SvrAreaId,MAX(AfterLevel)as MaxLevel from PlayerLvUpFlow
where date(dtEventTime)<=date(?date)  group by vopenid ,SvrAreaId)t1 
right outer join 
(select distinct vopenid,SvrAreaId from PlayerRegister where date(dtEventTime)<= date(?date)
and Channel=?Channel
and SvrAreaId=?SvrAreaId)t2 
on t1.vopenid=t2.vopenid and t1.SvrAreaId = t2.SvrAreaId");

       //某日等级变化的角色   
       RegisterSqlCommand((int)SqlCommand.SELECT_ALLPLAYER_LEVELUP,
    @"select MAX(AfterLevel)as level, vopenid,SvrAreaId 
    from PlayerLvUpFlow 
    where date(dtEventTime)=date(?date) group by vopenid ,SvrAreaId having SvrAreaId=?SvrAreaId");


       //某日登出角色信息 = 
       RegisterSqlCommand((int)SqlCommand.SELECT_LOGOUTPLAYER_LEVEL,
    @"select MAX(iLevel)as level, vopenid ,SvrAreaId 
    from PlayerLogout 
    where date(dtEventTime)=date(?date) 
    and Channel=?Channel
    and SvrAreaId=?SvrAreaId 
    group by vopenid ,SvrAreaId");






       //鲸鱼数据
       RegisterSqlCommand((int)SqlCommand.SELECT_RECHARGEPLAYER_DEVICEID,
           @"SELECT SystemHardware from PlayerLogin 
        where vopenid = ?openid 
        and Channel=?Channel
         and SvrAreaId=?SvrAreaId
        and dtEventTime <= ?date 
        order by dtEventTime desc LIMIT 1");


       RegisterSqlCommand((int)SqlCommand.SELECT_RECHARGEPLAYER_LEVEL,
           @"SELECT ifnull(Max(AfterLevel),1)as AfterLevel 
            from PlayerLvUpFlow 
            where vopenid= ?openid 
            and SvrAreaId = ?SvrAreaId
            and dtEventTime <= ?date");


       RegisterSqlCommand((int)SqlCommand.SELECT_RECHARGEPLAYER_DIAMOND_CONSUMCOST,
@" SELECT ifnull(sum(Price),0)as sum
from DiamondPayRecord 
where vopenid= ?openid 
and SvrAreaId = ?SvrAreaId
and dtEventTime <= ?date ");






        //////////////钻石Pay记录
       RegisterSqlCommand((int)SqlCommand.SELECT_DIAMONDPAY,
@"select DiamondBuyItemType,t1.vopenid,t1.SvrAreaId
from 
(select  DiamondBuyItemType,vopenid,SvrAreaId from  DiamondPayRecord 
where date(dtEventTime)=date(?date)
group by vopenid,SvrAreaId,DiamondBuyItemType)t1 ,
(select   distinct vopenid ,SvrAreaId from PlayerLogin where date(dtEventTime)=date(?date) 
and Channel=?Channel
and SvrAreaId=?SvrAreaId)t2  
where t1.vopenid = t2.vopenid  and t1.SvrAreaId = t2.SvrAreaId
");

       //////////////钻石Present记录
        RegisterSqlCommand((int)SqlCommand.SELECT_DIAMONDPRESENT,
@"select EventID,t1.vopenid,t1.SvrAreaId
from (select  EventID,vopenid,SvrAreaId from  DiamondPresentRecord 
where date(dtEventTime)=date(?date)
group by vopenid,SvrAreaId,EventID)t1 ,
(select   distinct vopenid ,SvrAreaId from PlayerLogin where date(dtEventTime)=date(?date) 
and Channel=?Channel
and SvrAreaId=?SvrAreaId)t2  
where t1.vopenid = t2.vopenid  and t1.SvrAreaId = t2.SvrAreaId
");
        



       //某日付费账号
       RegisterSqlCommand((int)SqlCommand.SELECT_PAY_ACCOUNT,
        @"SELECT player.ppid,ifnull(sum(price),0) as sum  
        FROM 
        order_data,
        player,
        product,
        (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
        WHERE order_data.playerid = player.playerid 
        AND order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid 
        AND date(order_data.`timestamp`) = date(?date) 
        AND  account.platformid=?Channel 
        AND  order_data.zoneid=?SvrAreaId
        group by player.ppid;");


       //SELECT_PAY_ACCOUNT_Before
       RegisterSqlCommand((int)SqlCommand.SELECT_PAY_ACCOUNT_Before,
     @"SELECT player.ppid,ifnull(sum(price),0) as sum  
        FROM 
        order_data,
        player,
        product,
        (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
        WHERE order_data.playerid = player.playerid 
        AND order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid 
        AND date(order_data.`timestamp`) < date(?date) 
        AND  account.platformid=?Channel 
        AND  order_data.zoneid=?SvrAreaId
        group by player.ppid;");


       RegisterSqlCommand((int)SqlCommand.SELECT_PAY_ACCOUNTBY_PPID,
      @"SELECT price  as sum  
        FROM 
        order_data,
        player,
        product,
        (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
        WHERE order_data.playerid = player.playerid 
        AND order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid 
        AND date(order_data.`timestamp`) = date(?date) 
        AND  account.platformid=?Channel 
        AND  order_data.zoneid=?SvrAreaId
        AND account.ppid = ?ppid");




        // 某日付费角色信息
       RegisterSqlCommand((int)SqlCommand.SELECT_PAY_Player,
        @"SELECT player.playerid,ifnull(SUM(product.price),0) as sum
        FROM 
		order_data,
		player,
		product,
		(SELECT ifnull(channelid,platformid)as platformid,ppid,nickname from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
        WHERE order_data.playerid = player.playerid 
        AND order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid 
        AND date(order_data.`timestamp`) = date(?date) 
        AND account.platformid=?Channel
        AND order_data.zoneid=?SvrAreaId
	    group by playerid");


       RegisterSqlCommand((int)SqlCommand.SELECT_PAY_Player_Before,
        @"SELECT player.playerid,ifnull(SUM(product.price),0) as sum
        FROM 
		order_data,
		player,
		product,
		(SELECT ifnull(channelid,platformid)as platformid,ppid,nickname from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
        WHERE order_data.playerid = player.playerid 
        AND order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid 
        AND date(order_data.`timestamp`) < date(?date) 
        AND account.platformid=?Channel
        AND order_data.zoneid=?SvrAreaId
	    group by playerid");

        



        //鲸鱼数据，查询充值用户信息
       RegisterSqlCommand((int)SqlCommand.SELECT_RechargeGamerInfo,
        @"SELECT account.ppid,max(player.diamond)as diamond,player.playerid,account.nickname,SUM(product.price )as sum,Max(order_data.timestamp) as `timestamp`
        FROM 
		order_data,
		player,
		product,
		(SELECT ifnull(channelid,platformid)as platformid,ppid,nickname from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
        WHERE order_data.playerid = player.playerid 
        AND order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid 
        AND date(order_data.`timestamp`) <= date(?date) 
        AND account.platformid=?Channel
        AND order_data.zoneid=?SvrAreaId
        AND player.playerid=?playerid
	    group by playerid");


        



       //个人充值
//       RegisterSqlCommand((int)SqlCommand.SELECT_AddUserRecharges,
//      @"SELECT order_data.orderid,account.ppid,player.diamond, player.zoneid,player.playerid,account.nickname,product.price ,order_data.timestamp as orderCreateTime,account.timestamp as accountCreateTime,
//        order_data.status
//        FROM 
//        order_data,
//        product,
//        player,
// (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
//LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
//        WHERE order_data.playerid = player.playerid 
//        AND player.ppid = account.ppid 
//        AND product.productid = order_data.productid 
//        AND order_data.`timestamp` > ?lastdate 
//        AND order_data.`timestamp` <= ?date
//        AND account.platformid=?Channel
//        AND order_data.zoneid=?SvrAreaId");



       RegisterSqlCommand((int)SqlCommand.SELECT_AddUserRecharges,
            @"SELECT
	order_data.orderid,
	order_data.ppid,
	order_data.playerid,
	order_data.zoneid,
	order_data.productid,
	order_data.`status`,
	order_data.`timestamp`  as orderCreateTime,
	order_third.pforderid,
	account.platformid,
	account.channelLabel,
    account.timestamp as accountCreateTime,
	channelLabel_lj.channelid,
	order_third.pforderid_sec,
	product.price,
	account.nickname
FROM
	order_data
LEFT JOIN order_third ON order_data.orderid = order_third.cporderid
LEFT JOIN account ON order_data.ppid = account.ppid
LEFT JOIN channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel
Left JOIN product on order_data.productid = product.productid
WHERE
	order_data.`timestamp` >?lastdate
	AND order_data.`timestamp` <= ?date
	AND order_data.zoneid=?SvrAreaId
	and (account.platformid=?Channel or channelLabel_lj.channelid=?Channel)");








         RegisterSqlCommand((int)SqlCommand.SELECT_AddUserRecharges_ChannelOrderNo,
            @"SELECT pforderid  from order_third where 
        cporderid =?orderid");




       //date1 - date2 所有付费账号付费次数
       RegisterSqlCommand((int)SqlCommand.SELECT_PAY_ACCOUNT_COUNT,
        @"SELECT account.ppid , count(account.ppid)as count   
        FROM 
        order_data,
        product,
        player,
         (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account 
        WHERE order_data.playerid = player.playerid 
        AND date(order_data.`timestamp`) >= date(?date1) 
        AND date(order_data.`timestamp`) <= date(?date2) 
        AND order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid  
        AND account.platformid=?Channel
        And order_data.zoneid=?SvrAreaId
        group by account.ppid");




       //某日第一次付费账号=某日付费账号-之前付费过的账号(左外连接)
       RegisterSqlCommand((int)SqlCommand.SELECT_FIRSTPAY_ACCOUNT,
           @"select tb1.ppid,tb1.sumtoday as sum
        from 
        (SELECT player.ppid,sum(price)as sumtoday  
        FROM 
        order_data,
        product,
        player,
         (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account 
        WHERE order_data.playerid = player.playerid 
    AND date(order_data.`timestamp`) = date(?date) 
    AND order_data.`status`=2 
    AND player.ppid = account.ppid 
    AND product.productid = order_data.productid  
    and account.platformid=?Channel 
    and order_data.zoneid=?SvrAreaId 
    group by player.ppid)tb1 
        left outer join
        (SELECT player.ppid,sum(price)as sumbefore 
        FROM 
        order_data,
        product,
        player,
        (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
        WHERE order_data.playerid = player.playerid AND date(order_data.`timestamp`) < date(?date) AND order_data.`status`=2 AND player.ppid = account.ppid AND product.productid = order_data.productid  and account.platformid=?Channel and order_data.zoneid=?SvrAreaId  group by player.ppid)tb2 
        on tb1.ppid=tb2.ppid where tb2.ppid is NULL;");



       //某日第一次付费角色=某日付费角色-之前付费过的角色(左外连接)
       RegisterSqlCommand((int)SqlCommand.SELECT_FIRSTPAY_PLAYER,
        @"select tb1.playerid,tb1.sumtoday as sum 
        from 
        (SELECT player.playerid,sum(price) as sumtoday  
    FROM order_data,
    product,
    player,
 (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
    WHERE order_data.playerid = player.playerid AND date(order_data.`timestamp`) = date(?date) AND order_data.`status`=2 AND player.ppid = account.ppid AND product.productid = order_data.productid  and account.platformid=?Channel 
            and  order_data.zoneid=?SvrAreaId group by player.playerid)tb1
        left outer join 
    (SELECT distinct player.playerid  
    FROM order_data,
    product,
    player,
 (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
WHERE order_data.playerid = player.playerid 
AND date(order_data.`timestamp`) < date(?date)
AND order_data.`status`=2 
AND player.ppid = account.ppid 
AND product.productid = order_data.productid 
and account.platformid=?Channel 
and  order_data.zoneid=?SvrAreaId)tb2    
on tb1.playerid=tb2.playerid where tb2.playerid is NULL ;");





        //某日第一次付费设备todo




       //某个ppid在date1 到date2 之间的付费
       RegisterSqlCommand((int)SqlCommand.SELECT_LTV_ACCOUNT_PAY,
        @"SELECT 
        ifnull(sum(price),0) as sum  
        FROM
        order_data,
        product,
        player,
 (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account
        WHERE order_data.playerid = player.playerid 
        AND date(order_data.`timestamp`) >= date(?date1) 
        AND date(order_data.`timestamp`) <= date(?date2) 
        And order_data.`status`=2 
        AND player.ppid = account.ppid 
        AND product.productid = order_data.productid  
        AND  player.ppid=?ppid 
        AND  account.platformid=?Channel
        AND order_data.zoneid=?SvrAreaId;");


        //某日某个ppid(账户)的付费
       RegisterSqlCommand((int)SqlCommand.SELECT_ACCOUNT_PAY,
           @"SELECT
        ifnull(sum(price),0) as sum  
        FROM 
        order_data,
        product,
        player,
 (SELECT ifnull(channelid,platformid)as platformid,ppid from account 
LEFT JOIN  channelLabel_lj ON account.channelLabel = channelLabel_lj.channelLabel)as account 
        WHERE  order_data.`status`=2
        AND product.productid = order_data.productid 
        AND date(order_data.`timestamp`) = date(?date)
        AND player.ppid = account.ppid  
        AND  order_data.ppid=?ppid 
        AND  account.platformid=?Channel  
         AND  order_data.zoneid=?SvrAreaId  ;"); 


  
        return true;
    }

    public  SqlStament GetSqlStament(SqlCommand command)
    {
        int icommand = (int)command;
       if(m_commandMap.ContainsKey(icommand))
       {
           if (icommand <(int) SqlCommand.MAX_DB_NETLOG)
           {
               return new SqlStament(m_commandMap[(int)command],m_netlogCon); 
           }
           if ((int)SqlCommand.MAX_DB_NETLOG < icommand  &&icommand < (int)SqlCommand.MAX_TDGAME_46001)
           {
               return new SqlStament(m_commandMap[(int)command], m_tdgameCon); 
           }
       }
       Log.LogError("sqlid:" + command + "not register");
       return null;
    }




    //执行sqlcommand,返回sql执行的结果
    //-1 出错
    //0 返回值为Null
    //1 成功
    public int SetAndExecute(SqlCommand cmd, ref DataTable resultTB, params MySql.Data.MySqlClient.MySqlParameter[] parsArray)
    {
        resultTB.Clear();
        try
        {
            SqlStament st = GetSqlStament(cmd);
            DataTable resultTb = new DataTable();
            if (st == null)
            {
                return -1;
            }
            foreach (var pars in parsArray)
            {
                st.SetParameter(pars);
            }
            string msg = "";
            if (!st.Execute(ref resultTB, ref msg))
            {
                Log.LogError("sqlid:" + cmd );
                Log.LogError("sqlcommand"+st.GetCommand());
                Log.LogError("reason:"+msg);
                return -1;
            }
        }
        catch (Exception e)
        {
            Log.LogError(e.ToString());
            return -1;
        }
        return 1;
    }






    //执行sqlcommand,返回sql执行的结果
    //-1 出错
    //0 返回值为Null
    //1 成功
    public int SetAndExecute(SqlCommand cmd, string resultName, ref string result,params MySql.Data.MySqlClient.MySqlParameter[] parsArray)
    {

        try
        {
            SqlStament st = GetSqlStament(cmd);
            DataTable resultTb = new DataTable();
            if (st == null)
            {
                Log.LogError("sql:" + st.GetCommand() + "not register");
                return -1;
            }
            foreach (var pars in parsArray)
            {
                st.SetParameter(pars);
            } 
           
            string msg = "";
            if (!st.Execute(ref resultTb, ref msg))
            {
                Log.LogError("sqlid:"+cmd+"sqlcommand" + st.GetCommand() + "execute error" + msg);

                return -1;
            }
            if (resultTb.Rows.Count <= 0)
            {
                return 0;
            }
            result = resultTb.Rows[0][resultName].ToString();
        }
        catch (Exception e)
        {
            Log.LogError(e.ToString());
            return -1;
        }
        return 1;
    }


   private void RegisterSqlCommand(int sqlCommand,string strCommand)
   {
       m_commandMap[sqlCommand] = strCommand;
   }


   private Dictionary<int, string> m_commandMap = new Dictionary<int, string>();

   private string m_netlogCon="";
   private string m_tdgameCon="";


}


public class SqlStament
{
    public SqlStament(string command, string connStr)
    {
        m_sqlCommand = command;
        m_connStr = connStr;
    }



    public void SetParameter(MySql.Data.MySqlClient.MySqlParameter paras)
    {
        m_sqlParams.Add(paras);
    }


    public void SetParameter(MySql.Data.MySqlClient.MySqlParameter[] parasArray)
    {
        foreach (MySql.Data.MySqlClient.MySqlParameter par in parasArray)
        {
            SetParameter(par);
        }
    }

    public string GetCommand()
    {
        return m_sqlCommand;
    }

    
    //执行sqlcommand,返回sql执行的结果
    public bool Execute(ref DataTable dataTable, ref string strErrMsg)
    {
        try
        {
            dataTable = MySqlHelper.GetDataSet(m_connStr, CommandType.Text, m_sqlCommand, m_sqlParams.ToArray()).Tables[0];
        }
        catch (Exception e)
        {
            strErrMsg = e.ToString();
            return false;
        }
        return true;
    }



 






    private string m_sqlCommand;
    private List<MySql.Data.MySqlClient.MySqlParameter> m_sqlParams=new List<MySql.Data.MySqlClient.MySqlParameter>();

    //连接字符串
    private string m_connStr;

}
