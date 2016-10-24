/ Read futures simulation results and save to database
/ @author Shen Feng
/ created on 2016.03.23
/
How to use:

/ Read and convert to snapshots
.sim.parseDir["/home/shen/git/futures_simulation/20160321-20160321"]
/2016.03.17 3781390
\

\d .sim

HDBDIR:@[value;`HDBDIR;"/mnt/kdb_data1/ModelAnalysisTemp1"]  / output
GW:@[value;`GW;`:192.168.1.41:9000:user1:password]

HDBCOLS:`date`day_night`exch`product`symbol`strategy`max_vol`para1`para2`para3`rounds`pnl`tick_dd`gross_pnl,
    `tot_trade_vol`tot_trade_amt`tot_order_vol`tot_cancel_vol`n_order`n_cancel


is_hdb:{[x] $[@[value;`.Q.pf;`rdb]~ `date; 1b; 0b]}
/ is_sim_result:{("KDB_"~4#x) & ((-12#x) like "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].csv")}
is_sim_result:{("KDB_"~4#x) & ((-4#x) like ".csv")}
reload:{system "l ",HDBDIR}

// Re-calculate PNL
refreshPnl:{[tbl]
    tbl:update pnl:`real$gross_pnl-tot_trade_amt*0.00008295 from tbl where product in `shrb`shbu`shhc;
    tbl:update pnl:`real$gross_pnl-tot_trade_amt*0.000191 from tbl where product=`dlpp;
    tbl:update pnl:`real$gross_pnl-tot_trade_amt*0.000572 from tbl where product in `dlj`dljm;
    tbl:update pnl:`real$gross_pnl-tot_trade_amt*0.0002383 from tbl where product=`dli;
    tbl:update pnl:`real$gross_pnl-tot_trade_vol*0.315 from tbl where product=`zzrm;
    tbl:update pnl:`real$gross_pnl-tot_trade_vol*0.7056 from tbl where product=`zzcf;
    tbl:update pnl:`real$gross_pnl-tot_trade_vol*0.159 from tbl where product=`dla;
    tbl:update pnl:`real$gross_pnl-tot_trade_vol*0.32 from tbl where product=`dll;
    tbl:update pnl:`real$gross_pnl-tot_trade_vol*0.1192 from tbl where product=`dlcs;
    tbl:update pnl:`real$gross_pnl-tot_trade_vol*0.1192 from tbl where product=`dlm;
    tbl:update pnl:`real$gross_pnl-tot_trade_vol*0.1986 from tbl where product=`dlp;
    tbl:update pnl:`real$gross_pnl-tot_trade_vol*0.1986 from tbl where product=`dly;

    tbl
  }

// Re-calculate PNL on disk
refreshHdbPnl:{[SimFutures;day]
    stdout "Starting to process ", string day;
    tbl:select from SimFutures where date=day;
	tbl:delete date from tbl;
    tbl:refreshPnl[tbl];
    stdout "Saving to disk";

    tblpath:` sv (hsym `$HDBDIR),(`$string day),`SimFutures,`;
    tblpath set .Q.en[hsym `$HDBDIR] tbl
 }

parseDir:{[dir]
    if[ -11h<>type dir;dir:hsym `$dir];
    system "mkdir -p ",HDBDIR;
    parseDirDate[dir] each (distinct {$[is_sim_result[string x];8#-12#string x;""]} each key dir) except enlist ""
  }

parseDirDate:{[dir;dt]
    stdout"Processing ",dt;
    if[ -11h<>type dir;dir:hsym `$dir];

    files:raze {[x;dt]$[dt ~ 8#-12#string x;x;()]}[;dt] each key dir;
    stdout"Number of files: ",(string count files);

    / break if file count is 0
    if[0>=count files; :(::)];

    simFutures: raze {[dir;file]
      / check file name is like KDB_czce_zzfg_20160321.csv
      $[is_sim_result[string file];
        ("dissssieeeeeeeefeeee";enlist",") 0:` sv dir,file;
        ()]
    }[dir;] each files;
	stdout "llllllllllllllllllllllllllllllll";
	stdout (string count files);
    simFutures:HDBCOLS xcol simFutures;
    stdout (string count simFutures);
    simFutures:delete from simFutures where not exch in `cffex`czce`dce`shfe;
    simFutures:delete from simFutures where tick_dd=0n;
    
	stdout (string count simFutures);
    tradeDates:getTradeDate[exec min date from simFutures; exec max date from simFutures];

    { tbl:select from y where date=x;
      if[is_hdb[];
        if[x in select date from `SimFutures;
          / replace existing records on disk according to `date`day_night`exch`product`symbol`strategy`max_vol`para1`para2`para3
          tbl:0!(10!select from `SimFutures where date=x) uj (10!tbl)]];
      tbl:delete from tbl where not exch in `cffex`czce`dce`shfe;
      tbl:delete from tbl where tick_dd=0n;
      tbl:delete date from tbl; / remove date column
      0N! "duandian1";
      // Calculate market volume percentage
      mktvol:getMarketVolume[x;z;exec distinct symbol from tbl];
      tbl:tbl lj mktvol;
      tbl:update mkt_vol:0 from tbl where mkt_vol = 0Ni; 
      tbl:update mkt_vol_pct:{$[0 = y;0.0;x % y]}'[tot_trade_vol;mkt_vol], alter_rounds:(tot_trade_vol%max_vol)%2 from tbl;
      0N! "duandian2";
      // Sort by these columns
      tbl:`exch`product`day_night`strategy`max_vol`para1`para2`para3 xasc select distinct from tbl;
      tbl:refreshPnl[tbl];

      stdout"Saving to disk";
      tblpath:` sv (hsym `$HDBDIR),(`$string x),`SimFutures,`;
	  stdout (string tblpath);
      tblpath set .Q.en[hsym `$HDBDIR] tbl;
      reload[];   / reload HDB
	  0N! "end!!!"
    }[;simFutures;tradeDates] each exec distinct date from simFutures;

    {[colm] @[value;(`.sim.setattrcol;hsym `$HDBDIR;`SimFutures;colm;`p);
      {-1 "Failed to set parted attr for ", y, ": ",x}[;colm]]
    } each `exch`product;
    reload[];
  }

serverFuncMap:`FuturesCffex`FuturesCzce`FuturesDce`FuturesShfe!(
    {[day;con] `Symbol`Category xkey update Category:0i from (select last Volume by Symbol from `cffexquote where date=day,Symbol in con)};
    {[day;con] update `long$Volume%2 from (select last Volume by Symbol,Category from `CzceDepth where date=day,Symbol in con)};
    {[day;con] update `long$Volume%2 from (select last Volume by Symbol,Category from `DceDepth where date=day,Symbol in con)};
    {[day;con] update `long$Volume%2 from (select last Volume by Symbol,Category from `ShfeDepth where date=day,Symbol in con)}
  )

// day is natural day (instead of trading day)
getMarketVolume:{[day;tradedates;con]
    wrapper:{[map;day;con] if[not .proc.proctype in key map;:()];.[map[.proc.proctype];(day;con)]};
    (neg gw:hopen GW) (`.gw.asyncexec;(wrapper;serverFuncMap;day;con);`FuturesCffex`FuturesCzce`FuturesDce`FuturesShfe);
    res:gw(::);

    nights:select from res where Category=1;
    res:update Volume:{[nights;s;dayv]
        nightv:exec first Volume from nights where Symbol=s;
        $[0N<>nightv;dayv-nightv;dayv]
      }[nights]'[Symbol;Volume] from res where Category=0;
    res:delete from res where Category=1;  // remove night session as it is last natural day's data

    // TODO get trade day for different exchanges, in case they are not the same any more
    day:exec min TRADE_DT from tradedates where TRADE_DT>day;  // get next trading day
    (neg gw) (`.gw.asyncexec;(wrapper;serverFuncMap;day;con);`FuturesCffex`FuturesCzce`FuturesDce`FuturesShfe);
    nights:gw(::);
    res,:delete from nights where Category = 0;
    hclose gw;
    // Rename columns
    `symbol`day_night`mkt_vol xcol res
 }

getTradeDate:{[start_day;end_day]
    gw:hopen GW;
    (neg gw) (`.gw.asyncexec;
        ({raze {update ex:z, "D"$string TRADE_DT from GetTradeDate[z;(x-15; y+15)]}[x;y]
         peach `CFFEX`DCE`CZCE`SHFE};start_day;end_day);
        `EquityFactor);
    res:gw(::);
    hclose gw;
    res
 }

//////////////////////////////
// Copied from dbmaint.q
//////////////////////////////
allcols:{[tabledir]get tabledir,`.d}

allpaths:{[dbdir;table]
 files:key dbdir;
 if[any files like"par.txt";:raze allpaths[;table]each hsym each`$read0(`)sv dbdir,`par.txt];
 files@:where files like"[0-9]*";(`)sv'dbdir,'files,'table}

fn1col:{[tabledir;col;fn]
 if[col in allcols tabledir;
  oldattr:-2!oldvalue:get p:tabledir,col;
  newattr:-2!newvalue:fn oldvalue;
  if[$[not oldattr~newattr;1b;not oldvalue~newvalue];
   stdout"resaving column ",(string col)," (type ",(string type newvalue),") in `",string tabledir;
   oldvalue:0;.[(`)sv p;();:;newvalue]]]}

fncol:{[dbdir;table;col;fn] / fncol[thisdb;`trade;`price;2*]
 fn1col[;col;fn]each allpaths[dbdir;table];}

setattrcol:{[dbdir;table;col;newattr] / setattr[thisdb;`trade;`sym;`g] / `s `p `u
 fncol[dbdir;table;col;newattr#]}

stdout:{-1 raze[" "sv string`date`second$.z.Z]," ",x;}

////////////////////////////////////////////////


\d .

.sim.reload[]

// If there is a parameter, use it to call parseDir
if[0 < count .z.x; .sim.parseDir[.z.x 0]; exit 0]
