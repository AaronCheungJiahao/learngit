ShfeDepthStructure:`ReceiveTime`TradingDate`SettleGroupID`SettleID`LastPrice`PreSettlePrice`PreClosePrice`PreOpenInterest`OpenPrice`HighestPrice`LowestPrice`Volume`Turnover`OpenInterest`ClosePrice`SettlePrice`UpperLimitPrice`LowerLimitPrice`PreDelta`CurrDelta`Time`Millisec`Symbol`BidPrice1`BidVol1`AskPrice1`AskVol1`BidPrice2`BidVol2`AskPrice2`AskVol2`BidPrice3`BidVol3`AskPrice3`AskVol3`BidPrice4`BidVol4`AskPrice4`AskVol4`BidPrice5`BidVol5`AskPrice5`AskVol5`CalendarDate`DataFlag`StartIndex`DataCount`BPrice1`BPrice2`BPrice3`BPrice4`BPrice5`BPrice6`BPrice7`BPrice8`BPrice9`BPrice10`BPrice11`BPrice12`BPrice13`BPrice14`BPrice15`BPrice16`BPrice17`BPrice18`BPrice19`BPrice20`BPrice21`BPrice22`BPrice23`BPrice24`BPrice25`BPrice26`BPrice27`BPrice28`BPrice29`BPrice30`BVol1`BVol2`BVol3`BVol4`BVol5`BVol6`BVol7`BVol8`BVol9`BVol10`BVol11`BVol12`BVol13`BVol14`BVol15`BVol16`BVol17`BVol18`BVol19`BVol20`BVol21`BVol22`BVol23`BVol24`BVol25`BVol26`BVol27`BVol28`BVol29`BVol30`SPrice1`SPrice2`SPrice3`SPrice4`SPrice5`SPrice6`SPrice7`SPrice8`SPrice9`SPrice10`SPrice11`SPrice12`SPrice13`SPrice14`SPrice15`SPrice16`SPrice17`SPrice18`SPrice19`SPrice20`SPrice21`SPrice22`SPrice23`SPrice24`SPrice25`SPrice26`SPrice27`SPrice28`SPrice29`SPrice30`SVol1`SVol2`SVol3`SVol4`SVol5`SVol6`SVol7`SVol8`SVol9`SVol10`SVol11`SVol12`SVol13`SVol14`SVol15`SVol16`SVol17`SVol18`SVol19`SVol20`SVol21`SVol22`SVol23`SVol24`SVol25`SVol26`SVol27`SVol28`SVol29`SVol30`TotalBidVol`TotalAskVol`BidWAvgPrice`AskWAvgPrice;
DailyStructure:`date`Symbol`PreSettlePrice`OpenPrice`HighestPrice`LowestPrice`ClosePrice`SettlePrice`Volume`Turnover`OpenInterest`Change`InterestChg`TradingDate;

///////////////////////////////////////////
// Common APIs
///////////////////////////////////////////
MAIN_TABLE:`ShfeDepth

\l code/mydb/storedproc/tickfactor.q
\l code/mydb/storedproc/common.q

///////////////////////////////////////////
// Custom APIs
///////////////////////////////////////////
JasonGetDataBySymbolDate:{[tb;day;code;scols;flag]
/ flag[0] is daynight
  tday:$[flag[0]=2;day;flag[0]=1;$[(day-`week$day)=4;day+3;day+1];day];
/ if scols ==ture means all columns value means all columns  ;else return your picking columns union Symbol
  scols1:$[((-4#(string scols))~"ture");(value scols);(count((`Symbol union scols) except scols))~0;scols;(`Symbol union scols)];
  tflag:$[flag[0]=2;(0 1);flag[0]=1;1;0];
 / get all symbol you pass in the function
  t1:select distinct Symbol from flip (raze `Symbol)!enlist raze code;
 / use your symbol union with the MainContract in today
  code:exec Symbol from t1 union (select Symbol from MainContract where MainFlag in (exec Symbol from t1),NextDate=tday);
/ return the data
  t:?[tb;((=;`date;tday);(in;`Symbol;enlist code);(in;`Category;tflag));0b;(raze scols1)!(raze scols1)];
  / flag[1]get top 100
  /if y=0 means all data
  y:flag[1];
  code:{$[0<=type x;x;enlist x]} code; /*raze code也可将原子转成数组
  / top only get y data not none.
  t:$[y~0;t;raze {[x;y;t] delete from (y#select from t where Symbol=x) where Symbol=`}[;y;t] each code];
  $[(-4#(string scols))~"ture";t;?[t;();0b;(raze scols)!(raze scols)]]};

JasonGetDataBySymbolCategoryDate:{[tb1;tb2;day;col;con;scols;flag]
 tday:$[flag[0]=2;day;flag[0]=1;$[(day-`week$day)=4;day+3;day+1];day];
 scols1:$[((-4#(string scols))~"ture");(value scols);(count((`Symbol union scols) except scols))~0;scols;(`Symbol union scols)];
 tflag:$[flag[0]=2;(0 1);flag[0]=1;1;0];
 code:?[tb2;((=;`NextDate;tday);(in;col;enlist con));();`Symbol]; /*结果为数组，(type x)>=0为数组，each只认数组
 t:?[tb1;((=;`date;tday);(in;`Symbol;enlist code);(in;`Category;tflag));0b;(raze scols1)!(raze scols1)];
 y:flag[1];
 t:$[y~0;t;raze {[x;y;t] y#select from t where Symbol=x}[;y;t] each code];
 $[(-4#(string scols))~"ture";t;?[t;();0b;(raze scols)!(raze scols)]]};

///////////////////////////////////////////
// APIs designed for C clients
///////////////////////////////////////////
ShfeDepthStructure2BytesFuncs:({
    (8#`byte$string x`TradingDate),0x00,(8#`byte$x`SettleGroupID),0x000000,
    (-4#-8!x`SettleID),(-8#-8!x`LastPrice),(-8#-8!x`PreSettlePrice),(-8#-8!x`PreClosePrice),
    (-8#-8!x`PreOpenInterest),(-8#-8!x`OpenPrice),(-8#-8!x`HighestPrice),(-8#-8!x`LowestPrice),
    (-4#-8!x`Volume),0x00000000,(-8#-8!x`Turnover),(-8#-8!x`OpenInterest),(-8#-8!x`ClosePrice),
    (-8#-8!x`SettlePrice),(-8#-8!x`UpperLimitPrice),(-8#-8!x`LowerLimitPrice),(-8#-8!x`PreDelta),
    (-8#-8!x`CurrDelta),(-8#-8!8#string "T"$-9#"00000000",string x`Time),0x00000000,
    (-4#-8!x`Millisec),(32#((9_-8!x`Symbol),0x0000000000000000000000000000000000000000000000000000000000000000)),
    (-8#-8!x`BidPrice1),(-4#-8!x`BidVol1),0x00000000,(-8#-8!x`AskPrice1),(-4#-8!x`AskVol1),0x00000000,
    (-8#-8!x`BidPrice2),(-4#-8!x`BidVol2),0x00000000,(-8#-8!x`AskPrice2),(-4#-8!x`AskVol2),0x00000000,
    (-8#-8!x`BidPrice3),(-4#-8!x`BidVol3),0x00000000,(-8#-8!x`AskPrice3),(-4#-8!x`AskVol3),0x00000000,
    (-8#-8!x`BidPrice4),(-4#-8!x`BidVol4),0x00000000,(-8#-8!x`AskPrice4),(-4#-8!x`AskVol4),0x00000000,
    (-8#-8!x`BidPrice5),(-4#-8!x`BidVol5),0x00000000,(-8#-8!x`AskPrice5),(-4#-8!x`AskVol5),
    (8#`byte$string x`CalendarDate),0x00000000,
    (-4#-8!x`DataFlag),(-2#-8!x`StartIndex),(-2#-8!x`DataCount)
 };{
    (-8#-8!x`BPrice1),(-8#-8!x`BPrice2),(-8#-8!x`BPrice3),(-8#-8!x`BPrice4),(-8#-8!x`BPrice5),(-8#-8!x`BPrice6),(-8#-8!x`BPrice7),(-8#-8!x`BPrice8),(-8#-8!x`BPrice9),(-8#-8!x`BPrice10),
    (-8#-8!x`BPrice11),(-8#-8!x`BPrice12),(-8#-8!x`BPrice13),(-8#-8!x`BPrice14),(-8#-8!x`BPrice15),(-8#-8!x`BPrice16),(-8#-8!x`BPrice17),(-8#-8!x`BPrice18),(-8#-8!x`BPrice19),(-8#-8!x`BPrice20),
    (-8#-8!x`BPrice21),(-8#-8!x`BPrice22),(-8#-8!x`BPrice23),(-8#-8!x`BPrice24),(-8#-8!x`BPrice25),(-8#-8!x`BPrice26),(-8#-8!x`BPrice27),(-8#-8!x`BPrice28),(-8#-8!x`BPrice29),(-8#-8!x`BPrice30),
    (-4#-8!x`BVol1),(-4#-8!x`BVol2),(-4#-8!x`BVol3),(-4#-8!x`BVol4),(-4#-8!x`BVol5),(-4#-8!x`BVol6),(-4#-8!x`BVol7),(-4#-8!x`BVol8),(-4#-8!x`BVol9),(-4#-8!x`BVol10),
    (-4#-8!x`BVol11),(-4#-8!x`BVol12),(-4#-8!x`BVol13),(-4#-8!x`BVol14),(-4#-8!x`BVol15),(-4#-8!x`BVol16),(-4#-8!x`BVol17),(-4#-8!x`BVol18),(-4#-8!x`BVol19),(-4#-8!x`BVol20),
    (-4#-8!x`BVol21),(-4#-8!x`BVol22),(-4#-8!x`BVol23),(-4#-8!x`BVol24),(-4#-8!x`BVol25),(-4#-8!x`BVol26),(-4#-8!x`BVol27),(-4#-8!x`BVol28),(-4#-8!x`BVol29),(-4#-8!x`BVol30)
 };{
    (-8#-8!x`SPrice1),(-8#-8!x`SPrice2),(-8#-8!x`SPrice3),(-8#-8!x`SPrice4),(-8#-8!x`SPrice5),(-8#-8!x`SPrice6),(-8#-8!x`SPrice7),(-8#-8!x`SPrice8),(-8#-8!x`SPrice9),(-8#-8!x`SPrice10),
    (-8#-8!x`SPrice11),(-8#-8!x`SPrice12),(-8#-8!x`SPrice13),(-8#-8!x`SPrice14),(-8#-8!x`SPrice15),(-8#-8!x`SPrice16),(-8#-8!x`SPrice17),(-8#-8!x`SPrice18),(-8#-8!x`SPrice19),(-8#-8!x`SPrice20),
    (-8#-8!x`SPrice21),(-8#-8!x`SPrice22),(-8#-8!x`SPrice23),(-8#-8!x`SPrice24),(-8#-8!x`SPrice25),(-8#-8!x`SPrice26),(-8#-8!x`SPrice27),(-8#-8!x`SPrice28),(-8#-8!x`SPrice29),(-8#-8!x`SPrice30),
    (-4#-8!x`SVol1),(-4#-8!x`SVol2),(-4#-8!x`SVol3),(-4#-8!x`SVol4),(-4#-8!x`SVol5),(-4#-8!x`SVol6),(-4#-8!x`SVol7),(-4#-8!x`SVol8),(-4#-8!x`SVol9),(-4#-8!x`SVol10),
    (-4#-8!x`SVol11),(-4#-8!x`SVol12),(-4#-8!x`SVol13),(-4#-8!x`SVol14),(-4#-8!x`SVol15),(-4#-8!x`SVol16),(-4#-8!x`SVol17),(-4#-8!x`SVol18),(-4#-8!x`SVol19),(-4#-8!x`SVol20),
    (-4#-8!x`SVol21),(-4#-8!x`SVol22),(-4#-8!x`SVol23),(-4#-8!x`SVol24),(-4#-8!x`SVol25),(-4#-8!x`SVol26),(-4#-8!x`SVol27),(-4#-8!x`SVol28),(-4#-8!x`SVol29),(-4#-8!x`SVol30),
    (-4#-8!x`TotalBidVol),(-4#-8!x`TotalAskVol),
    (-8#-8!x`BidWAvgPrice),(-8#-8!x`AskWAvgPrice)
 })

ShfeDepthStructure2Bytes:{raze {y@x}[x] each ShfeDepthStructure2BytesFuncs}

GetByteBySymbolCategoryDateTime:{[tb;ref_tb;day;ref_col;code;time;scols;flag]
    tbl:GetDataBySymbolCategoryDateTime[tb;ref_tb;day;ref_col;code;time;scols;flag];
    tbl:update Byte:(ShfeDepthStructure2Bytes peach tbl) from tbl;
    // Remove columns except ReceiveTime.
    `ReceiveTime`Byte xcols ![tbl;();0b;ShfeDepthStructure except `ReceiveTime]
 }

//////////////
// Load data
//////////////
reload:{.lg.o[`reload;"reloading ",hdbdir];system "l ",hdbdir};
/ reload database periodically
.timer.repeat["p"$.z.D + 17:07:00;0Wp;0D24:00:00;(`reload;());"reload database"];
.timer.repeat["p"$.z.D + 07:40:00;0Wp;0D24:00:00;(`reload;());"reload database"];
reload[];
