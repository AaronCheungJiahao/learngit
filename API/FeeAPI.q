GW: `:192.168.1.41:9000:user1:password;
h:hopen GW;
(neg h) ".gw.asyncexec[\" select from FeeRate where date=last date \"; `FeeRateDB]"; dce:h(::);
hclose h;

//////////////////////////////////1GetFeeRateData//////////////////////////

/ "{[day;flag]
/     tday:$[flag=2;day;flag=1;$[(day-`week$day)=4;day+3;day+1];day];
/     select from FeeRate where date=tday,Category=flag
/   }"

GW: `:192.168.1.41:9000:user1:password;
h:hopen GW;
(neg h) (`.gw.asyncexec;(`GetFeeRateData;2016.07.01;1);`FeeRateDB);resdd:h(::);
hclose h;



//////////////////////////////////2GetLatestFeeRate//////////////////////////
GW: `:192.168.1.41:9000:user1:password;
h:hopen GW;
(neg h) (`.gw.asyncexec;(`GetLatestFeeRate;());`FeeRateDB);resdd:h(::);
hclose h;






//////////////////////////////////////////3GetFeeRateDataByDate///////////////////////////////////////////
/ 
/ "{[day;flag]
/     select from FeeRate where date=day,Category=flag
/   }"

GetFeeRateDataByDate

GW: `:192.168.1.41:9000:user1:password;
h:hopen GW;
(neg h) (`.gw.asyncexec;(`GetFeeRateDataByDate;2016.07.01;0);`FeeRateDB);resdd:h(::);
hclose h;




/////////////////////////////////////////4GetPeriodFeeRateData///////////////////////////////////////////////
/ "{[f;t;flag]
/     tday:{[day;flag] $[flag=2;day;flag=1;$[(day-`week$day)=4;day+3;day+1];day]} [;flag] each (f;t);
/     select from FeeRate where date within tday,Category=flag
/   }"
