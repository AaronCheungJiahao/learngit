

system "l /home/data/prod_script/daily/swjscript/mailhelper/mail2.q"
system"l /mnt/kdb_data1/TradeLog/DB";

\l /home/data/kdb/code/common/getdate.q


sendTradeLogAfternoon:{
    [tday]
  rList:("zhangjiahao@mycapital.net";"shenweijun@mycapital.net");
  GW: `:192.168.1.41:9000:user1:password;
  h:hopen GW;
  sqlStr: "select from TradeLog where date=",string (tday);  
  (neg h) (`.gw.asyncexec; sqlStr ;`TradeLogDB); tbl:h(::);
  DayNum:count select from tbl where DayNight=0;
  NightNum:count select from tbl where DayNight=1;
  hclose h;
  res:([] dbName:`TradeLogDB`TradeLogDB;tblName:`TradeLog`TradeLog;DATE:(tday;tday);DayNight:(0;1);Num:(DayNum;NightNum));
  res:update style__:`green from res;
  res:update style__:`red from res where Num=0;  
  $[0 in res`Num;head:"Afternoon Report !!TradeLog Data Error ![Fail!!!!]";head:"Afternoon Report !! TradeLog Data Succeed[Success!!!!]"];
  ret:.util.sendReportQuick[head;res;rList];
  0N! "send!!!";
  }

sendTradeLogAfternoon .z.D
exit 0
