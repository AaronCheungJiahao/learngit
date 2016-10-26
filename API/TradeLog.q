GW: `:192.168.1.41:9000:user1:password;
h:hopen GW;
(neg h) ".gw.asyncexec[\" select from TradeLog where date=2016.08.05, DayNight=1 \"; `TradeLogDB]"; res8:h(::);
hclose h;

