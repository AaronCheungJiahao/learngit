GW: `:192.168.1.41:9000:user1:password;
h:hopen GW;
(neg h) ".gw.asyncexec[(`GetLatestFeeRate;()); `FeeRateDB]"; res3:h(::);
hclose h;

tblFee:select Product,MyProduct,ExchCode ,FeeMode,SimuFee,TickSize,TradeUnit from res3
tmpFee:update FeeMode:{$[x=`ByAmt;0;1]} '[FeeMode] from  update Type:0 from tblFee