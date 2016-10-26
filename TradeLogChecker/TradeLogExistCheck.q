system "l /home/data/prod_script/daily/swjscript/mailhelper/mail.q"


getTradingDateList:{
  [sday;eday]
  
  get_trading_date_sql:"GetTradeDate[`CFFEX`SHFE`CZCE`DCE; (",(string sday),";",(string eday),")]";
  
  GW: `:192.168.1.41:9000:user1:password;
  h:hopen GW;
  (neg h) (`.gw.asyncexec; get_trading_date_sql ;(`EquityFactor)); tdList:h(::);
  hclose h;
  
  exec TRADE_DT from update TRADE_DT:"D"$string TRADE_DT from tdList
  };

GetMktDataStatus:{
  [tabNames;dbNames;tday;categoryNum;i]
  GW: `:192.168.1.41:9000:user1:password;
  h:hopen GW;
  
  sqlStr: "select from ",(string (tabNames i))," where date=",(string tday),", DayNight=",(string categoryNum);
  if[(dbNames i) like "TradeLogA50";sqlStr: "select from ",(string (tabNames i))," where date=",(string tday),", Category=",(string categoryNum)];
  
  (neg h) (`.gw.asyncexec; sqlStr ;dbNames i); tbl:h(::);
  hclose h;
  res: ([]dbName:enlist (dbNames i);tableName:enlist (tabNames i);TRADE_DT:tday;CATEGORY: enlist (categoryNum);n:count tbl;flag:$[(0 = count tbl);`NO;`YES])
  };
  
sendReport:{
  [tbl;assetType;tdate]
  
  n1:count select from tbl where flag = `NO;
  futTbl:update style__:`green from tbl;
  if[n1>0;futTbl:update style__:`red from futTbl where flag =`NO];
  
  mailhead:(string assetType)," Data Existence Check";
  mailbody:"Updated Status on ",(string tdate);
  $[n1>0;mailhead:"[Fail!!!] Data For ",(string assetType);mailhead:"[succeed] Data For ",(string assetType)];
  
  receiverList:("shenweijun@mycapital.net";"huangyf@mycapital.net";"weilw@mycapital.net");
  REPORTER:`:192.168.1.41:9022:user1:password;
  ret: REPORTER (`sendReportQuick; mailhead;   (mailbody;"";futTbl); (receiverList));
  while[(ret < 0); 0N! "resending..."; ret: REPORTER (`sendReportQuick; mailhead;   (mailbody;"";futTbl); (receiverList)); system "sleep 10"]
  };

tabNames:enlist `TradeLog;
dbNames:enlist `TradeLogDB;
assetType:`TradeLog;

args : .z.x;
if[1>count args; 0N! "Please specify argument parameter firstly!!!"; exit 0];
categoryNum:"I"$(args 0);
tdate:.z.D;
if[1=categoryNum;tdate:first getTradingDateList[tdate;tdate+10]];

allTab: raze {
  [tabNames;dbNames;tdate;categoryNum;i] 
  total:GetMktDataStatus[tabNames;dbNames;tdate;categoryNum;i]
  } [tabNames;dbNames;tdate;categoryNum;] each til count tabNames;

tabDic:()!();
tabDic[`TradeLog]:allTab;
rList:("shenweijun@mycapital.net";"huangyf@mycapital.net";"weilw@mycapital.net");
ret:.util.sendMail[string assetType;rList;tabDic];


exit 0;
