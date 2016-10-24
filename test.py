# test.py
# encoding: utf-8

import sys
from qpython import qconnection

def get_mkt_data():
        q_sql=".gw.asyncexec[(`GetDataBySymbolCategoryDateTime;`DceDepth;();2016.10.19;();`a1701;();`DceDepthStructure;(1;-1)); `FuturesDce]"
        q.async(q_sql)
        mc=q.receive(pandas=True)
        print mc
        return mc

def getZZ800StockList(dateStr):
        sList = []
        addr, port, user, pwd, db, tbl = "192.168.1.41", 9000, 'superuser1', 'password', 'FuturesShfe', 'ShfeDepth'
        with qconnection.QConnection(host=addr,port=port,username=user,password=pwd) as q:
            print 'connnected to KDB. reading MainContract' 
            queryStr = '.gw.asyncexec[(`GetConstituentStock;(`ZZ800;`NUL); (%s;%s)); `EquityFactor]' %(dateStr, dateStr)
            print queryStr
            q.async(queryStr)
            tupList =q.receive(pandas=True)
            for tup in tupList:
                sList.append(tup[0])
                return sList


def getAllStockList(dateStr):
        sList = []
        addr, port, user, pwd, db, tbl = "192.168.1.41", 9000, 'superuser1', 'password', 'EquityFactor', 'AShareEODPrices'
        with qconnection.QConnection(host=addr,port=port,username=user,password=pwd) as q:
            print 'connnected to KDB. reading AShareEODPrices' 
       
            queryStr = '.gw.asyncexec["select distinct SYMBOL from AShareEODPrices where TRADE_DT=' + dateStr + '";`' + db + ']' 
            print "query:" + queryStr
            
            q.async(queryStr)
            tupList =q.receive(pandas=True)
            for tup in tupList:
                sList.append(tup[0])
        
        return sList


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    
    print 'ZZ stock list:'
    zzStockList = getZZ800StockList("2016.10.21")
    print zzStockList
    

    print 'All stock list:'
    allStockList = getAllStockList("2016.10.21")
    print allStockList
