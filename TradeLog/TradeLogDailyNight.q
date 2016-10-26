isOldFile:{
  [filePath]
  strList:read0 filePath;
  if[0 = count strList;:1b];
  (4 = count ("." vs ((read0 filePath) 0)))
   };

isEmptyFile:{
  [filePath]
  strList:read0 filePath;
  (0 = count strList)
   };

fields:`InternalDate`CalendarDate`DayNight`Time`MicroSec`Server`Account`ModelId`MsgType,`SerialNo`CancelSerialNo`Symbol`Direction`OpenClose`OrderPrice`OrderVol`EntrustNo`EntrustStatus`TradePrice`TradeVol`VolRemain`TradeNo`Speculator`OrderKind`OrderType`ErrorNo;
  rawFields:`CalendarDate`Time`Server`MsgType,`k1`v1`k2`v2`k3`v3`k4`v4`k5`v5`k6`v6`k7`v7`k8`v8`k9`v9`k10`v10`k11`v11`k12`v12`k13`v13`k14`v14`k15`v15;
  fieldMapping:1!flip `original`target!"ss"$\:();
  fieldMapping,:((`account_no;`Account);      (`serial_no;`SerialNo);           (`cancel_serial_no;`CancelSerialNo);
              (`direction;`Direction);      (`open_close;`OpenClose);         (`speculator;`Speculator);
              (`order_kind;`OrderKind);     (`order_type;`OrderType);         (`error_no;`ErrorNo);
              (`entrust_no;`EntrustNo);     (`entrust_status;`EntrustStatus); (`volume_remain;`VolRemain);
              (`stock_code;`Symbol);        (`limit_price;`OrderPrice);       (`volume;`OrderVol);
              (`business_volume;`TradeVol); (`business_price;`TradePrice);    (`business_no;`TradeNo));
map:{$[x in key fieldMapping;fieldMapping[x][`target];x]};           

saveOldFiles:{
  [p;D;x;dir]
         tradelog:flip fields!();
         tradelog,:raze {
         tmp_raw:flip rawFields!("ssssssssssssssssssssssssssssssssss";" ")0:x;
         tmp_result::flip fields!"ssssssssssssssssssssssssss"$\:();
         ip:exec first CalendarDate from tmp_raw where (Time=`) & (MsgType=`);
         tmp_raw:update Server:ip from tmp_raw;
         tmp_raw:delete from tmp_raw where (Time=`) & (MsgType=`);
         tmp_raw:update .Q.fu[{`$-1_'1_'string x};MsgType],  / remove first [ and last ]
                .Q.fu[{map each `$-1_'string x};k1], .Q.fu[{`$-1_'string x};v1],  / remove last ;:
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
                .Q.fu[{map each `$-1_'string x};k15], .Q.fu[{`$'string x};v15] from tmp_raw;
       {h:4#x; d:2 cut 4 _ value x;k:(first each d) except `; v:enlist each (last each d) except `;tmp_result::tmp_result uj flip h,k!v} each tmp_raw;
       update DayNight:{h:`hh$"T"$string x;`int$$[(7<=h) & 19 > h; 0; 1]}'[Time] from `tmp_result;
       update Symbol:{$[x<>`;exec first Symbol from tmp_result where SerialNo=x,Symbol<>`;y<>`;exec first Symbol from tmp_result where CancelSerialNo=y,Symbol<>`;`]}'[SerialNo;CancelSerialNo] from `tmp_result where Symbol=`;
       if[`org_serial_no in cols tmp_result;
                                          update SerialNo:org_serial_no, CancelSerialNo:SerialNo from `tmp_result where org_serial_no<>`,MsgType=`CancelOrder;
                                          update SerialNo:`, CancelSerialNo:SerialNo from `tmp_result where MsgType=`CancelRespond;
                                          delete org_serial_no from `tmp_result];
      tmp_result::tmp_result lj 2!select CancelSerialNo,Symbol,SerialNo from tmp_result where ((SerialNo <> `) & (CancelSerialNo <> `));
      update InternalDate:CalendarDate,ModelId:SerialNo from `tmp_result;
      update ModelId:CancelSerialNo from tmp_result where ModelId=`} each dir;
      tradelog:update .Q.fu[{"I"$(string x) except\:"-"};InternalDate],
                      .Q.fu[{"I"$(string x) except\:"-"};CalendarDate],
                       string Time,
                       MicroSec:`long$((`long$"N"$(string Time))%1000),
                      .Q.fu[{"I"$-8#'string x};ModelId],
                      .Q.fu[{"F"$'string x};SerialNo],
                      .Q.fu[{"F"$'string x};CancelSerialNo],
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
       0N!"the old tradelog will be deal with!";
       tradelog:update Trigger:`NA from tradelog
       };
       ///////////////////////////////////////////////////////////////////////////////////////////
       ///////////////////////////////////////////////////////////////////////////////////////////
  saveNewFile:{
  [tmp_raw]
  fieldMapping:1!flip `original`target!"ss"$\:();
  fieldMapping,:((`account;`Account); 
               (`server;`Server); 
               (`model_id;`ModelId); 
               (`serial_no;`SerialNo); 
               (`cancel_serial_no;`CancelSerialNo);
               (`symbol;`Symbol);   
               (`direction;`Direction);    
               (`open_close;`OpenClose); 
               (`order_price;`OrderPrice);
               (`order_vol;`OrderVol); 
               (`entrust_no;`EntrustNo);  
               (`entrust_status;`EntrustStatus);
               (`trade_price;`TradePrice);
               (`trade_vol;`TradeVol)  ;
               (`vol_remain;`VolRemain);
               (`trade_no;`TradeNo);           
               (`speculator;`Speculator);
               (`order_kind;`OrderKind);    
               (`order_type;`OrderType);  
               (`error_no;`ErrorNo);
               (`trigger;`Trigger));
               
       map:{$[x in key fieldMapping;fieldMapping[x][`target];x]};
            0N! "Before process, file lines=",(string (count tmp_raw));
            fieldMapping:1!flip `original`target!"ss"$\:();
            fieldMapping,:((`account;`Account); 
                           (`server;`Server); 
                           (`model_id;`ModelId); 
                           (`serial_no;`SerialNo); 
                           (`cancel_serial_no;`CancelSerialNo);
                           (`symbol;`Symbol);   
                           (`direction;`Direction);    
                           (`open_close;`OpenClose); 
                           (`order_price;`OrderPrice);
                           (`order_vol;`OrderVol); 
                           (`entrust_no;`EntrustNo);  
                           (`entrust_status;`EntrustStatus);
                           (`trade_price;`TradePrice);
                           (`trade_vol;`TradeVol)  ;
                           (`vol_remain;`VolRemain);
                           (`trade_no;`TradeNo);           
                           (`speculator;`Speculator);
                           (`order_kind;`OrderKind);    
                           (`order_type;`OrderType);  
                           (`error_no;`ErrorNo);
                           (`trigger;`Trigger));  
            map:{$[x in key fieldMapping;fieldMapping[x][`target];x]};
            tmp_raw:update  .Q.fu[{`$-1_'1_'string x};MsgType],  / remove first [ and last ]
                            .Q.fu[{map each `$-1_'string x};k1], .Q.fu[{`$-1_'string x};v1],  / remove last ;:
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
                            .Q.fu[{map each `$-1_'string x};k15], .Q.fu[{`$-1_'string x};v15],
                            .Q.fu[{map each `$-1_'string x};k16], .Q.fu[{`$-1_'string x};v16],
                            .Q.fu[{map each `$-1_'string x};k17], .Q.fu[{`$-1_'string x};v17],
                            .Q.fu[{map each `$-1_'string x};k18], .Q.fu[{`$-1_'string x};v18],
                            .Q.fu[{map each `$-1_'string x};k19], .Q.fu[{`$-1_'string x};v19],
                            .Q.fu[{map each `$-1_'string x};k20], .Q.fu[{`$-1_'string x};v20],
                            .Q.fu[{map each `$-1_'string x};k21], .Q.fu[{`$ string x};v21] from tmp_raw;
            tmp_vv:"v",/:string 1+til 21;
            tmp_k:"k",/:string 1+til 21;
            0N! tmp_vv;
            0N! tmp_k;
            tmp_kk:("Server";"Account";"ModelId";"SerialNo";"CancelSerialNo";"Symbol";"Direction";"OpenClose";"OrderPrice";"OrderVol";"EntrustNo";"EntrustStatus";"TradePrice";"TradeVol";"VolRemain";"TradeNo";"Speculator";"OrderKind";"OrderType";"ErrorNo";"Trigger");
            0N! tmp_kk;
            sqlStr:"update Server:v1, Account:v2, ModelId:v3, SerialNo:v4, CancelSerialNo:v5, Symbol:v6, Direction:v7, OpenClose:v8, OrderPrice:v9, OrderVol:v10, EntrustNo:v11, EntrustStatus:v12, TradePrice:v13, TradeVol:v14, VolRemain:v15, TradeNo:v16, Speculator:v17, OrderKind:v18, OrderType:v19, ErrorNo:v20, Trigger:v21 from tmp_raw";
            0N! sqlStr;
            tmp_result:update Server:v1, Account:v2, ModelId:v3, SerialNo:v4, CancelSerialNo:v5, Symbol:v6, Direction:v7, OpenClose:v8, OrderPrice:v9, OrderVol:v10, EntrustNo:v11, EntrustStatus:v12, TradePrice:v13, TradeVol:v14, VolRemain:v15, TradeNo:v16, Speculator:v17, OrderKind:v18, OrderType:v19, ErrorNo:v20, Trigger:v21 from tmp_raw;
            0N! "www";
            t:delete v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21,k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k14, k15, k16, k17, k18, k19, k20, k21,Other from tmp_result;
            t:update DayNight:{h:`hh$"T"$string x;`int$$[(7<=h) & 19 > h; 0; 1]}'[Time] from t;
            t:update InternalDate:CalendarDate from t;
            tradelog:update .Q.fu[{"I"$(string x) except\:"-"};InternalDate],
                                  .Q.fu[{"I"$(string x) except\:"-"};CalendarDate],
                                   string Time,
                                   MicroSec:`long$((`long$"N"$(string Time))%1000),
                                  .Q.fu[{"I"$'string x};ModelId],
                                  .Q.fu[{"F"$'string x};SerialNo],
                                  .Q.fu[{"F"$'string x};CancelSerialNo],
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
                                  .Q.fu[{`$'string x };Trigger],
                                  .Q.fu[{first each string x};EntrustStatus] from t;
                                  0N! "after process, file lines=",(string (count tradelog));
                                  0N!"the new tradelog will be deal with!";
                                  tradelog
                      };
p:`:/mnt/DataProject/TunnelLogs;
D:desc key p;
D:2#D;

x:0;
dir:` sv (` sv p,D x),`Ngt; /*` sv (` sv p,D x),`tradelog;
dir:dir {` sv x,y}'key dir;
dir:dir where (string dir) like "*.log";
ttTbl:([]FilePath:dir);
ttTbl:update IsOldFile:{isOldFile[x]}'[FilePath] from ttTbl;
ttTbl:update IsEmptyFile:{isEmptyFile[x]}'[FilePath] from ttTbl;
ttTbl:select from ttTbl where IsEmptyFile = 0b;

oldDir:exec FilePath from ttTbl where IsOldFile = 1b; 
newDir:exec FilePath from ttTbl where IsOldFile = 0b;
         
oldTab:saveOldFiles[p;D;x;oldDir];

newFileTab:raze {
    [filePath]
    rawFields:`CalendarDate`Time`Other`MsgType,`k1`v1`k2`v2`k3`v3`k4`v4`k5`v5`k6`v6`k7`v7`k8`v8`k9`v9`k10`v10`k11`v11`k12`v12`k13`v13`k14`v14`k15`v15`k16`v16`k17`v17`k18`v18`k19`v19`k20`v20`k21`v21;
    tmp_raw:flip rawFields!("ssssssssssssssssssssssssssssssssssssssssssssss";" ")0:filePath;
    saveNewFile[tmp_raw] 
    } each newDir;
             
            
$[(count newFileTab)<>0;         
    [
    my_cols:`InternalDate`CalendarDate`DayNight`Time`MicroSec`Server`Account`ModelId`MsgType`SerialNo`CancelSerialNo`Symbol`Direction`OpenClose`OrderPrice`OrderVol`EntrustNo`EntrustStatus`TradePrice`TradeVol`VolRemain`TradeNo`Speculator`OrderKind`OrderType`ErrorNo`Trigger;
    newFileTab:my_cols xcols newFileTab;
    newFileTab:newFileTab,oldTab;
    ];
    newFileTab:oldTab];



  $[(("D"$string D x)-`week$("D"$string D x)) =4;
                                                     dbdir:` sv `:/mnt/kdb_data1/TradeLog/Night,(`$string("D"$string D x)+3),`TradeLog,`$"";
                                                     dbdir:` sv `:/mnt/kdb_data1/TradeLog/Night,(`$string("D"$string D x)+1),`TradeLog,`$""];
  dbdir set .Q.en[`:/mnt/kdb_data1/TradeLog/DB] update `p#Symbol from `Symbol`Time xasc newFileTab;

{ @[{if[-11h=type x; (hsym `$":" sv (y;last ":" vs string x;"user1:password"))"\\l ."]}[;x]each;
  @[hsym `$":" sv (x;"9000";"user1:password");"exec hpup from .servers.SERVERS where proctype=`TradeLogDB";-2@];-2@]
 } "192.168.1.41";
 
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



x:1;
dir:` sv (` sv p,D x),`Ngt; /*` sv (` sv p,D x),`tradelog;
dir:dir {` sv x,y}'key dir;
dir:dir where (string dir) like "*.log";
ttTbl:([]FilePath:dir);
ttTbl:update IsOldFile:{isOldFile[x]}'[FilePath] from ttTbl;
ttTbl:update IsEmptyFile:{isEmptyFile[x]}'[FilePath] from ttTbl;
ttTbl:select from ttTbl where IsEmptyFile = 0b;

oldDir:exec FilePath from ttTbl where IsOldFile = 1b; 
newDir:exec FilePath from ttTbl where IsOldFile = 0b;
         
oldTab:saveOldFiles[p;D;x;oldDir];

newFileTab:raze {
    [filePath]
    rawFields:`CalendarDate`Time`Other`MsgType,`k1`v1`k2`v2`k3`v3`k4`v4`k5`v5`k6`v6`k7`v7`k8`v8`k9`v9`k10`v10`k11`v11`k12`v12`k13`v13`k14`v14`k15`v15`k16`v16`k17`v17`k18`v18`k19`v19`k20`v20`k21`v21;
    tmp_raw:flip rawFields!("ssssssssssssssssssssssssssssssssssssssssssssss";" ")0:filePath;
    saveNewFile[tmp_raw] 
    } each newDir;
             
  $[(count newFileTab)<>0;         
    [
    my_cols:`InternalDate`CalendarDate`DayNight`Time`MicroSec`Server`Account`ModelId`MsgType`SerialNo`CancelSerialNo`Symbol`Direction`OpenClose`OrderPrice`OrderVol`EntrustNo`EntrustStatus`TradePrice`TradeVol`VolRemain`TradeNo`Speculator`OrderKind`OrderType`ErrorNo`Trigger;
    newFileTab:my_cols xcols newFileTab;
    newFileTab:newFileTab,oldTab;
    ];
    newFileTab:oldTab];



$[(("D"$string D x)-`week$("D"$string D x)) =4;
                                                     dbdir:` sv `:/mnt/kdb_data1/TradeLog/Night,(`$string("D"$string D x)+3),`TradeLog,`$"";
                                                     dbdir:` sv `:/mnt/kdb_data1/TradeLog/Night,(`$string("D"$string D x)+1),`TradeLog,`$""];
  dbdir set .Q.en[`:/mnt/kdb_data1/TradeLog/DB] update `p#Symbol from `Symbol`Time xasc newFileTab;

/ /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

{ @[{if[-11h=type x; (hsym `$":" sv (y;last ":" vs string x;"user1:password"))"\\l ."]}[;x]each;
  @[hsym `$":" sv (x;"9000";"user1:password");"exec hpup from .servers.SERVERS where proctype=`TradeLogDB";-2@];-2@]
 } "192.168.1.41"
 
 
exit 0;

/ 
/ system "l /mnt/kdb_data1/TradeLog/DB"
/ 
/ count select from TradeLog where date=2016.10.19,DayNight=1
