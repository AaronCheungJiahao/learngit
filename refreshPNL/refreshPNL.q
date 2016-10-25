system "l /mnt/kdb_data1/ModelAnalysisTemp";

HDBDIR:@[value;`HDBDIR;"/mnt/kdb_data1/ModelAnalysisTemp"];  / output

refreshPnl:{[tbl]
    0N!"begin to refresh";
    GW: `:192.168.1.41:9000:user1:password;
    h:hopen GW;
    (neg h) (`.gw.asyncexec;(`GetLatestFeeRate;());`FeeRateDB);feedata:h(::);
    hclose h;
    tt:tbl lj 1!`product`SimuFee`FeeMode xcols delete MyProduct from update product:MyProduct from feedata;
    tt:update pnl:`real$gross_pnl-tot_trade_amt*SimuFee from tt ;
    tt1:update pnl:`real$gross_pnl-tot_trade_amt*SimuFee from select from tt where FeeMode=`ByAmt;
    tt2:update pnl:`real$gross_pnl-tot_trade_vol*SimuFee from select from tt where FeeMode<>`ByAmt;
    tt:delete SimuFee,FeeMode from tt1,tt;
    col:`day_night`exch`product`symbol`strategy`max_vol`para1`para2`para3`rounds`pnl`tick_dd`gross_pnl,
    `tot_trade_vol`tot_trade_amt`tot_order_vol`tot_cancel_vol`n_order`n_cancel;
    col xcols tt
  };
 
refreshHdbPnl:{[SimFutures;day]
    0N! "Starting to process ", string day;
    tbl:select from SimFutures where date=day;
	tbl:delete date from tbl;
    tbl:refreshPnl[tbl];
    0N! "Saving to disk";
    tblpath:` sv (hsym `$HDBDIR),(`$string day),`SimFutures,`;
    0N! tblpath;
    tblpath set .Q.en[hsym `$HDBDIR] tbl
   };
 
refreshHdbPnl [SimFutures] each .Q.pv;

{ @[{if[-11h=type x; (hsym `$":" sv (y;last ":" vs string x;"user1:password"))"\\l ."]}[;x]each;
  @[hsym `$":" sv (x;"9000";"user1:password");"exec hpup from .servers.SERVERS where proctype=`ModelAnalysisVariable";-2@];-2@]
  } "192.168.1.41";

{ @[{if[-11h=type x; (hsym `$":" sv (y;last ":" vs string x;"user1:password"))"\\l ."]}[;x]each;
  @[hsym `$":" sv (x;"9000";"user1:password");"exec hpup from .servers.SERVERS where proctype=`ModelAnalysisVariable";-2@];-2@]
  } "192.168.1.44"
  
 exit 0
 