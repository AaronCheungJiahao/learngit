getSimFutures:{ $[98h=@[{type value x};`.cache.SimFutures;0Nh]; `.cache.SimFutures;`SimFutures] }

rawData:{[prod;str;start_day;end_day;daynight]
    select from SimFutures where date within (start_day;end_day),product=prod,day_night=daynight,strategy=str
  }

rawParaData:{[prod;str;start_day;end_day;daynight;p1;p2;p3]
    select from SimFutures where date within (start_day;end_day),product=prod,day_night=daynight,strategy=str,para1=`real$p1,para2=`real$p2,para3=`real$p3
  }

rawFourParaData:{[args]
    prod:args 0; str:args 1; start_day:args 2; end_day:args 3; daynight:args 4;
    mvol:args 5; p1:args 6; p2:args 7; p3:args 8;
    select from SimFutures where date within (start_day;end_day),product=prod,day_night=daynight,strategy=str,max_vol=`int$mvol,para1=`real$p1,para2=`real$p2,para3=`real$p3
  }

calPerfCore:{[tbl]
    0!select last min_para1, last max_para1, last min_para2, last max_para2, last min_para3, last max_para3,
      last avg_alter_rounds,last tot_pnl,last max_dd,last tot_pnl_max_dd_ratio, avg_pnl_per_round:avg pnl_per_round,
      avg_tick_dd:avg tick_dd, avg_tot_cancel_vol:avg tot_cancel_vol,avg_tot_order_vol:avg tot_order_vol,
      avg_n_cancel:avg n_cancel,avg_n_order:avg n_order,avg_mkt_vol_pct:avg mkt_vol_pct
      by from_date,to_date,n_days,day_night,exch,product,strategy,max_vol,para1,para2,para3 from tbl
  }

calPerfCore1:{[tbl]
    update tot_pnl_max_dd_ratio:tot_pnl%abs max_dd, pnl_per_round:pnl%alter_rounds
        from update dd:tot_pnl-cummax_tot_pnl,max_dd:mins tot_pnl-cummax_tot_pnl by max_vol,para1,para2,para3
        from update cummax_tot_pnl:(1_maxs (0,tot_pnl)) by max_vol,para1,para2,para3
        from update tot_pnl:sums pnl,avg_rounds:avg rounds,avg_alter_rounds:avg alter_rounds by max_vol,para1,para2,para3
        from update min_para1:min para1, max_para1:max para1, min_para2:min para2, max_para2:max para2, min_para3:min para3, max_para3:max para3
        from update n_days:count distinct date from tbl
  }

// Calculate parameters' performance (intermediate)
calPerf1:{[prod;str;start_day;end_day;daynight]
    tbl:update from_date:start_day, to_date:end_day from rawData[prod;str;start_day;end_day;daynight];
    calPerfCore1[tbl]
  }

calParaPerf1:{[prod;str;start_day;end_day;daynight;p1;p2;p3]
    tbl:update from_date:start_day, to_date:end_day from rawParaData[prod;str;start_day;end_day;daynight;p1;p2;p3];
    calPerfCore1[tbl]
  }

// Calculate parameters' performance
calPerf:{[prod;str;start_day;end_day;daynight]
    calPerfCore calPerf1[prod;str;start_day;end_day;daynight]
  }

calParaPerf:{[prod;str;start_day;end_day;daynight;p1;p2;p3]
    calPerfCore calParaPerf1[prod;str;start_day;end_day;daynight;p1;p2;p3]
  }

// Find neighbors of an element in a list
findNbr:{[list;x]
    list:$[0h>type list;enlist list;list];
    idx:list?x;
    low:$[idx=0;idx;idx-1];
    high:$[idx=(count list)-1;idx;idx+1];
    (list[low],x,list[high])
  }

// Calculate the neighbor average to find the optimal parameter combinations
calOptParams:{[prod;str;start_day;end_day;daynight;threshold_round;threshold_ratio]
    params:exec asc distinct max_vol, asc distinct para1, asc distinct para2, asc distinct para3
        from tbl:calPerf[prod;str;start_day;end_day;daynight];
    tbl:$[0<count select from tbl where avg_alter_rounds > threshold_round, tot_pnl_max_dd_ratio > threshold_ratio;
    `nbr_avg_tot_pnl xdesc select from
    (update nbr_avg_tot_pnl:{[tbl;params;str;v;p1;p2;p3]
      // filters
      f_p1:$[str like "hi5*"; raze p1; findNbr[params[`para1]; p1]];
      f_p2:findNbr[params[`para2]; p2];
      f_p3:findNbr[params[`para3]; p3];
	
      avg exec tot_pnl from tbl where max_vol = v,
        para1 in f_p1,
        para2 in f_p2,
        para3 in f_p3
      }[tbl;params]'[strategy;max_vol;para1;para2;para3],
      nbr_avg_max_dd:{[tbl;params;str;v;p1;p2;p3]
      // filters
      f_p1:$[str like "hi5*"; raze p1; findNbr[params[`para1]; p1]];
      f_p2:findNbr[params[`para2]; p2];
      f_p3:findNbr[params[`para3]; p3];

      avg exec max_dd from tbl where max_vol = v,
        para1 in f_p1,
        para2 in f_p2,
        para3 in f_p3
      }[tbl;params]'[strategy;max_vol;para1;para2;para3] from tbl where avg_alter_rounds > threshold_round, tot_pnl_max_dd_ratio > threshold_ratio)
    where nbr_avg_tot_pnl>0;
    // If there is no matching record, return empty table
    0#update nbr_avg_tot_pnl:0f,nbr_avg_max_dd:0f from 1#tbl];
    // Reorder nbr_avg_tot_pnl nbr_avg_max_dd columns
    (-2_(-7_cols tbl),`nbr_avg_tot_pnl`nbr_avg_max_dd,-7#cols tbl) xcols tbl
  }

calOptParamsOld:{[prod;str;start_day;end_day;daynight;threshold_round;threshold_ratio]
    params:exec asc distinct max_vol, asc distinct para1, asc distinct para2, asc distinct para3
        from tbl:calPerf[prod;str;start_day;end_day;daynight];
    tbl:$[0<count select from tbl where avg_alter_rounds > threshold_round, tot_pnl_max_dd_ratio > threshold_ratio;
    `nbr_avg_tot_pnl xdesc select from
    (update nbr_avg_tot_pnl:{[tbl;params;v;p1;p2;p3]
      avg exec tot_pnl from tbl where max_vol in findNbr[params[`max_vol]; v],
        para1 in findNbr[params[`para1]; p1],
        para2 in findNbr[params[`para2]; p2],
        para3 in findNbr[params[`para3]; p3]
      }[tbl;params]'[max_vol;para1;para2;para3],
      nbr_avg_max_dd:{[tbl;params;v;p1;p2;p3]
        avg exec max_dd from tbl where max_vol in findNbr[params[`max_vol]; v],
        para1 in findNbr[params[`para1]; p1],
        para2 in findNbr[params[`para2]; p2],
        para3 in findNbr[params[`para3]; p3]
      }[tbl;params]'[max_vol;para1;para2;para3] from tbl where avg_alter_rounds > threshold_round, tot_pnl_max_dd_ratio > threshold_ratio)
    where nbr_avg_tot_pnl>0;
    // If there is no matching record, return empty table
    0#update nbr_avg_tot_pnl:0f,nbr_avg_max_dd:0f from 1#tbl];
    // Reorder nbr_avg_tot_pnl nbr_avg_max_dd columns
    (-2_(-7_cols tbl),`nbr_avg_tot_pnl`nbr_avg_max_dd,-7#cols tbl) xcols tbl
  }

// Product, strategy, start date, end date
optParams:{[prod;str;start_day;end_day;daynight]
    calOptParams[prod;str;start_day;end_day;daynight;5;5]
  }

// Limit number of outputs
optParamsN:{[prod;str;start_day;end_day;daynight;n]
    res:optParams[prod;str;start_day;end_day;daynight];
    $[n<=count res;n#res;res]
  }

// Product, strategy, start date, end date
optParamsT:{[prod;str;start_day;end_day;daynight;threshold_round;threshold_ratio]
    calOptParams[prod;str;start_day;end_day;daynight;threshold_round;threshold_ratio]
  }

// Limit number of outputs
optParamsTN:{[prod;str;start_day;end_day;daynight;threshold_round;threshold_ratio;n]
    res:optParamsT[prod;str;start_day;end_day;daynight;threshold_round;threshold_ratio];
    $[n<=count res;n#res;res]
  }


////////////////////////////////////////////
// Output all strategies for a product
////////////////////////////////////////////

getStrategy:{[prod;start_day;end_day;daynight]
    (select distinct strategy from SimFutures where date within (start_day;end_day),product=prod,day_night=daynight)`strategy
  }

calProdPerf:{[prod;start_day;end_day;daynight]
    raze calPerf[prod;;start_day;end_day;daynight] peach getStrategy[prod;start_day;end_day;daynight]
  }

optProdParams:{[prod;start_day;end_day;daynight]
    raze optParams[prod;;start_day;end_day;daynight] peach getStrategy[prod;start_day;end_day;daynight]
  }

optProdParamsN:{[prod;start_day;end_day;daynight;n]
    raze optParamsN[prod;;start_day;end_day;daynight;n] peach getStrategy[prod;start_day;end_day;daynight]
  }

optProdParamsT:{[prod;start_day;end_day;daynight;threshold_round;threshold_ratio]
    raze optParamsT[prod;;start_day;end_day;daynight;threshold_round;threshold_ratio] peach getStrategy[prod;start_day;end_day;daynight]
  }

optProdParamsTN:{[prod;start_day;end_day;daynight;threshold_round;threshold_ratio;n]
    raze optParamsTN[prod;;start_day;end_day;daynight;threshold_round;threshold_ratio;n] peach getStrategy[prod;start_day;end_day;daynight]
  }

rawProdData:{[prod;start_day;end_day;daynight]
    select from SimFutures where date within (start_day;end_day),product=prod,day_night=daynight
  }

/////////////////////////////////////////
// Functions to check data completeness
/////////////////////////////////////////

checkProdStrCount:{[start_day;end_day]
    days:{x where (x mod 7) within 2 6} start_day + til 1+end_day-start_day;
    ref:3!delete date from select neg count pnl by date,product,strategy,day_night from SimFutures where date = start_day;
    delete pnl from (uj/) {[x;ref] res:ref pj 3!delete date from select count pnl by date,product,strategy,day_night from SimFutures where date = x;
        (`product`strategy`day_night,`$string x) xcol res}[;ref] peach days
 }

checkMissing1:{[start_day;end_day]
    days:{x where (x mod 7) within 2 6} start_day + til 1+end_day-start_day;
    ref:3!delete date from select neg count pnl by date,product,strategy,day_night from SimFutures where date = start_day;
    delete pnl from (uj/) {[x;ref] res:ref pj 3!delete date from select count pnl by date,product,strategy,day_night from SimFutures where date = x;
        $[0<count exec pnl from res where pnl<>0; (`product`strategy`day_night,`$string x) xcol res; res]
        }[;ref] peach days
 }

checkMissing:{[start_day;end_day]
    res:checkMissing1[start_day;end_day];
    delete s from select from (update s:(sum 3_value flip 0!res) from res) where s<0
 }
 hdbdir

reload:{.lg.o[`reload;"reloading ",hdbdir];system "l ",hdbdir};
/ reload database periodically
.timer.repeat["p"$.z.D + 06:40:00;0Wp;0D24:00:00;(`reload;());"reload database"];

reload[];
