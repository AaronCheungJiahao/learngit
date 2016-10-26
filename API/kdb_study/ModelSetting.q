//ModelSetting

GetModelSetting:{[day;flag]
//=2 day and night;=1:night;=0;day
	tday:$[flag=2;day;flag=1;$[(day-`week$day)=4;day+3;day+1];day];
/if flag=2 return list(0;1);elseif 1 reuturn 1 else return 0
	tflag:$[flag=2;(0 1);flag=1;1;0];
    / ? is select select from ModelSettings where date =tday,DayNight in tflga ;0b;(columns)
	t:?[ModelSettings;((=;`date;tday);(in;`DayNight;tflag));0b;()];
/ let all columns to symbol to reduce the space
	update `$ModelName,`$SoName,`$TradeStartTime,`$TradeCloseTime,`$OpenEndTime,`$Para1,`$Para2,`$Para3,`$FeeByVol,`$FeeByAmt,`$FillRatio from t
 }

/ reload 
reload:{.lg.o[`reload;"reloading ",hdbdir];system "l ",hdbdir};
reload[];
/ crontab task 16:15:00 and 06:35:00 reload the database!
.timer.repeat["p"$.z.D + 16:15:00;0Wp;0D24:00:00;(`reload;());"reload database"];
.timer.repeat["p"$.z.D + 06:35:00;0Wp;0D24:00:00;(`reload;());"reload database"];
          
          
 