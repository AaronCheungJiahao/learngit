MyTradeLog:{[day;flag]
          tday:$[flag=2;day;flag=1;$[(day-`week$day)=4;day+3;day+1];day];
          tflag:$[flag=2;(0 1);flag=1;1;0];
          t:?[TradeLog;((=;`date;tday);(in;`DayNight;tflag));0b;()];
          update `$Time from t}

reload:{.lg.o[`reload;"reloading ",hdbdir];system "l ",hdbdir}
reload[];

////////////////////////

fields:`InternalDate`CalendarDate`Category`Time`MicroSec`Server`Account`ModelId`MsgType,`SerialNo`CancelSerialNo`Symbol`Direction`OpenClose`OrderPrice`OrderVol`EntrustNo`EntrustStatus,
 `TradePrice`TradeVol`VolRemain`TradeNo`Speculator`OrderKind`OrderType`ErrorNo;
rawFields:`CalendarDate`Time`Server`MsgType,`k1`v1`k2`v2`k3`v3`k4`v4`k5`v5`k6`v6`k7`v7`k8`v8`k9`v9`k10`v10`k11`v11`k12`v12`k13`v13`k14`v14`k15`v15;
fieldMapping:1!flip `original`target!"ss"$\:();
fieldMapping,:((`account_no;`Account);      (`serial_no;`SerialNo);           (`cancel_serial_no;`CancelSerialNo);
              (`direction;`Direction);      (`open_close;`OpenClose);         (`speculator;`Speculator);
              (`order_kind;`OrderKind);     (`order_type;`OrderType);         (`error_no;`ErrorNo);
              (`entrust_no;`EntrustNo);     (`entrust_status;`EntrustStatus); (`volume_remain;`VolRemain);
              (`stock_code;`Symbol);        (`limit_price;`OrderPrice);       (`volume;`OrderVol);
              (`business_volume;`TradeVol); (`business_price;`TradePrice);    (`business_no;`TradeNo));
map:{$[x in key fieldMapping;fieldMapping[x][`target];x]};

/ get latest doc name in the directory
latest_doc:{doc:desc key x; doc: first doc where (string doc) like "201*"}

/ read dir and parse trade log
/ @param x:path
/ @param daynight
readdir:{[x; daynight]
         dir:` sv (` sv tradelog_dir,first x),$[`night=daynight;`tradelog`night;`tradelog];
         dir:dir {` sv x,y}'key dir;
         dir:dir where (string dir) like "*.log";
         tradelog:flip fields!();
         tradelog,:raze {
         	tmp_raw:flip rawFields!("ssssssssssssssssssssssssssssssssss";" ")0:x;
         	tmp_result::flip fields!"ssssssssssssssssssssssssss"$\:();
         	ip:exec first CalendarDate from tmp_raw where (Time=`) & (MsgType=`);
         	tmp_raw:update Server:ip from tmp_raw;
         	tmp_raw:delete from tmp_raw where (Time=`) & (MsgType=`);
         	tmp_raw:update .Q.fu[{`$-1_'1_'string x};MsgType],                                / remove first [ and last ]
         	               .Q.fu[{map each `$-1_'string x};k1], .Q.fu[{`$-1_'string x};v1],   / remove last ;:
                           .Q.fu[{map each `$-1_'string x};k2], .Q.fu[{`$-1_'string x};v2],
                           .Q.fu[{map each `$-1_'string x};k3], .Q.fu[{`$-1_'string x};v3],
                           .Q.fu[{map each `$-1_'string x};k4], .Q.fu[{`$-1_'string x};v4],
                           .Q.fu[{map each `$-1_'string x};k5], .Q.fu[{`$-1_'string x};v5],
                           .Q.fu[{map each `$-1_'string x};k6], .Q.fu[{`$-1_'string x};v6],
                           .Q.fu[{map each `$-1_'string x};k7], .Q.fu[{`$-1_'string x};v7],
                           .Q.fu[{map each `$-1_'string x};k8], .Q.fu[{`$-1_'string x};v8],
                           .Q.fu[{map each `$-1_'string x};k9], .Q.fu[{`$-1_'string x};v9],
                           .Q.fu[{map each `$-1_'string x};k10], .Q.fu[{`$-1_'string x};v10],
                           .Q.fu[{map each `$-1_'string x};k11], .Q.fu[{`$-1_'string x};v11],
                           .Q.fu[{map each `$-1_'string x};k12], .Q.fu[{`$-1_'string x};v12],
                           .Q.fu[{map each `$-1_'string x};k13], .Q.fu[{`$-1_'string x};v13],
                           .Q.fu[{map each `$-1_'string x};k14], .Q.fu[{`$-1_'string x};v14],
                           .Q.fu[{map each `$-1_'string x};k15], .Q.fu[{`$-1_'string x};v15] from tmp_raw;
            {h:4#x; d:2 cut 4 _ value x;k:(first each d) except `; v:enlist each (last each d) except `;tmp_result::tmp_result uj flip h,k!v} each tmp_raw;
            update Category:{h:`hh$"T"$string x;`int$$[(7<=h) & 19 > h; 0; 1]}'[Time] from `tmp_result;
            update Symbol:{$[x<>`;exec first Symbol from tmp_result where SerialNo=x,Symbol<>`;y<>`;exec first Symbol from tmp_result where CancelSerialNo=y,Symbol<>`;`]}'[SerialNo;CancelSerialNo] from `tmp_result where Symbol=`;
            if[`org_serial_no in cols tmp_result;
                update SerialNo:org_serial_no, CancelSerialNo:SerialNo from `tmp_result where org_serial_no<>`,MsgType=`CancelOrder;
                update SerialNo:`, CancelSerialNo:SerialNo from `tmp_result where MsgType=`CancelRespond;
                delete org_serial_no from `tmp_result];
            tmp_result::tmp_result lj 2!select CancelSerialNo,Symbol,SerialNo from tmp_result where ((SerialNo <> `) & (CancelSerialNo <> `));
            update InternalDate:CalendarDate, ModelId:SerialNo from `tmp_result;
            update ModelId:CancelSerialNo from tmp_result where ModelId=`} each dir;
         tradelog:update .Q.fu[{"I"$(string x) except\:"-"};InternalDate],
                         .Q.fu[{"I"$(string x) except\:"-"};CalendarDate],
                         string Time, MicroSec:`long$((`long$"N"$(string Time))%1000),
                         .Q.fu[{"I"$-3#'string x};ModelId],
                         .Q.fu[{"I"$'string x};SerialNo],
                         .Q.fu[{"I"$'string x};CancelSerialNo],
                         .Q.fu[{"I"$'string x};OpenClose],
                         .Q.fu[{"I"$'string x};OrderVol],
                         .Q.fu[{"I"$'string x};TradeVol],
                         .Q.fu[{"I"$'string x};VolRemain],
                         .Q.fu[{"I"$'string x};TradeNo],
                         .Q.fu[{"I"$'string x};Direction],
                         .Q.fu[{"I"$'string x};Speculator],
                         .Q.fu[{"I"$'string x};OrderKind],
                         .Q.fu[{"I"$'string x};OrderType],
                         .Q.fu[{"I"$'string x};ErrorNo],
                         .Q.fu[{"F"$'string x};OrderPrice],
                         .Q.fu[{"F"$'string x};TradePrice],
                         .Q.fu[{"F"$'string x};EntrustNo],
                         .Q.fu[{first each string x};EntrustStatus] from tradelog;
         dbdir:$[`night=daynight;
                 $[(("D"$string first x)-`week$("D"$string first x)) =4;
                   ` sv hdbNightdir,(`$string("D"$string first x)+3),`TradeLog,`$"";
                   ` sv hdbNightdir,(`$string("D"$string first x)+1),`TradeLog,`$""];
                 ` sv hdbDaydir,(`$string"D"$string first x),`TradeLog,`$""];
         dbdir set .Q.en[hdbDBdir] $[0<count tradelog;update `p#Symbol from `Symbol`Time xasc tradelog;tradelog]}

produce_daily:{[daynight] readdir[enlist latest_doc[tradelog_dir];daynight]}

/ Join data from TradeLog and EquityDepth, fill a50 with EquityDepth data
GetTradelogA50Equity:{[dt]
  gw:.util.gw[];
  a50_tb: select from `TradeLog where date = dt;
  symlist: distinct a50_tb`Symbol;
  a50_tb:update ReceiveTime:.util.dt2unixus[date;Time], Time:.util.time2int[Time] from a50_tb;
  (neg gw) (`.gw.asyncexec; "select from `EquityDepth where date=", (string dt), ", Symbol in ",.util.sym2str[symlist];`EquityDB);
  equity_tb:gw(::);
  res:aj[`Symbol`ReceiveTime; a50_tb; equity_tb]
 }

// get one variaty tradelog info whose MsgType = PlaceOrder
GetOrdMap:{[prod;day;dn]
  scols:`date`InternalDate`DayNight`Time`Symbol`Server`Account`ModelId`SerialNo`MsgType`Direction`OpenClose`OrderPrice`OrderVol`OrderType`Speculator;
  if [not dn in (0;1;2); '"Incorrect input, param dn should be 0, 1 or 2"];
  $[dn=2; tb:?[`TradeLog;((=;`date; day);(=;`MsgType; enlist `PlaceOrder));0b;(scols!scols)];
   tb:?[`TradeLog;((=;`date; day);(=;`DayNight; dn);(=;`MsgType; enlist `PlaceOrder));0b;(scols!scols)]];
  prod:`$ upper string prod;
  tb:update ProdID:{`$ (string x) except "0123456789"} each Symbol, SerialNo:7h$ SerialNo from tb;
  tb:select from tb where ProdID=prod
 }

/ reload database periodically
.timer.repeat["p"$.z.D + 17:15:00;0Wp;0D24:00:00;(`reload;());"reload database"];
.timer.repeat["p"$.z.D + 07:35:00;0Wp;0D24:00:00;(`reload;());"reload database"];
