system "l /home/data/prod_script/daily/swjscript/mailhelper/mail2.q"
system"l /mnt/kdb_data1/TradeLog/DB";

\l /home/data/kdb/code/common/getdate.q
  
//   Morning:
sendTradeLogMorning:{
    [tday]
  rList:enlist("zhangjiahao@mycapital.net");
  GW: `:192.168.1.41:9000:user1:password;
  h:hopen GW;
  sqlStr: "select from TradeLog where date=",(string (tday)),",DayNight=1";  
  (neg h) (`.gw.asyncexec; sqlStr ;`TradeLogDB); tbl:h(::);
  NightNum:count select from tbl where DayNight=1;
  sqlStr: "select from TradeLog where date=",(string (getLastTradingDateBefore tday)),",DayNight=0"; 
  (neg h) (`.gw.asyncexec; sqlStr ;`TradeLogDB); tbl:h(::); 
  DayNum:count select from tbl where DayNight=0;
  hclose h;
  res:([] dbName:`TradeLogDB`TradeLogDB;tblName:`TradeLog`TradeLog;DATE:(tday;(getLastTradingDateBefore tday));DayNight:(1;0);Num:(NightNum;DayNum));
  res:update style__:`green from res;
  res:update style__:`red from res where Num=0;  
  $[0 in res`Num;head:"Morning Report! TradeLog Data Error ![Fail!!!!]";head:"Morning Report! TradeLog Data Succeed[Success!!!!]"];
  ret:.util.sendReportQuick[head;res;rList];
}

.
sendTradeLogMorning .z.D