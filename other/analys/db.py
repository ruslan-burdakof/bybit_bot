import pandas as pn
import numpy as np
import sqlite3
import pickle

def db2pkl(symbol, interval ="1m", start = 4, pathDB = 'query_kline.db'):
    ''' Миграция из базы данных в pkl '''
    # Загрузка из базы данных
    con = sqlite3.connect(pathDB)
    data = pn.read_sql("select * from " +
                    symbol + "_" + interval + 
                    " ORDER BY startTime", con)
    data = data[start:]
    # Поиск прерываний в записях
    diff = data.startTime.diff()
    limit = diff[5]
    time = data.startTime[diff != limit]
    inx = iter(np.append(time.index, data.tail(1).index+1))
    next(inx)
    prt = 0
    for final in inx:
        prt = prt + 1
        final = final - 1
        print(f"Part {prt}: {start}...{final} len: {final-start+1}")
        dat = data[start:final]
        startTime= np.array(dat.startTime)
        openPr   = np.array(dat.open)
        highPr   = np.array(dat.high)
        lowPr    = np.array(dat.low)
        closePr  = np.array(dat.close)
        volume   = np.array(dat.volume)
        with open(f"{symbol}_{interval}_prt{prt}.pkl", "wb") as file:
            pickle.dump((startTime, openPr,
                        highPr, lowPr,
                        closePr, volume), file)
        start = final+1



from pybit.spot import HTTP
import sqlite3

def existTable(symbol_, interval_ = "", pathDB = 'query_kline.db'):
    ''' Проверка существования таблицы в базе данных '''
    sqlRequest = "SELECT NAME FROM sqlite_master WHERE type='table' AND name="
    if len(interval_) == 0:
        sqlRequest += f"'{symbol_}'"
    else:
        sqlRequest += f"'{symbol_}_{interval_}'"
    #print(sqlRequest)
    sqlite_connection = sqlite3.connect('query_kline.db')
    cursor  = sqlite_connection.cursor()
    cursor.execute(sqlRequest)
    if cursor.fetchone() == None:
        return False
    return True
def createTable(symbol_, interval_ = "", pathDB = 'query_kline.db'):
    sqlite_connection = sqlite3.connect(pathDB)
    cursor  = sqlite_connection.cursor()
    sqlRequest = '''CREATE TABLE '''
    if len(interval_) == 0:
        sqlRequest = f"CREATE TABLE {symbol_}"
    else:
        sqlRequest = f"CREATE TABLE {symbol_}_{interval_}"
    sqlRequest +=  '''(startTime BIGINT PRIMARY KEY UNIQUE NOT NULL,
                       open             REAL,
                       high             REAL,
                       low              REAL,
                       close            REAL,
                       volume           REAL,
                       endTime          BIGINT,
                       quoteAssetVolume REAL,
                       trades           INT,
                       takerBaseVolume  REAL,
                       takerQuoteVolume REAL);'''
    cursor.execute(sqlRequest)
    #print(cursor.fetchone())

def query_kline(data):
    for ans in data:
        yield (ans[0],                  #startTime [long]. Start time, unit in millisecond
               float(ans[1]),           #open     [float]. Open price
               float(ans[2]),           #high     [float]. High price
               float(ans[3]),           #low      [float]. Low price
               float(ans[4]),           #close    [float]. Close price
               float(ans[5]),           #volume   [float]. Trading volume
               ans[6],                  #endTime   [long]. End time, unit in millisecond
               float(ans[7]),           #quoteAssetVolume [float]. Quote asset volume
               ans[8],                  #trades           [int]. Number of trades
               float(ans[9]),           #takerBaseVolume  [float]. Taker buy volume in base asset
               float(ans[10]))          #takerQuoteVolume [float]. Taker buy volume in quote asset 

def lastTimeOnDB(table, pathDB = 'query_kline.db'):
    sqlite_connection = sqlite3.connect(pathDB)
    cursor = sqlite_connection.cursor()
    select="select max(startTime) from " + table
    cursor.execute(select)
    return cursor.fetchone()[0]

def lastTimeOnServer(client):
    return client.server_time()['result']['serverTime']

def updateDataBybit(symbol_, interval_, pathDB = "query_kline.db"):
    client = HTTP(endpoint='https://api.bybit.com')
    sqlite_connection = sqlite3.connect(pathDB)
    cursor  = sqlite_connection.cursor()
    table = symbol_+"_"+interval_
    if not existTable(symbol_, interval_):
        createTable(symbol_, interval_)
    try:
        sqlite_connection = sqlite3.connect(pathDB)
        cursor  = sqlite_connection.cursor()
        data = client.query_kline(symbol = symbol_, interval = interval_, limit = 2)
        step = data['result'][1][0] - data['result'][0][0]
        todb_ = lastTimeOnDB(table, pathDB)
        tosr_ = lastTimeOnServer(client)
        try:
            limit_ = int(tosr_/step)-int(todb_/step)
        except:
            limit_ = 1000
        if limit_ == 0:
            print("Данные актуальны")
            return 1
        data = client.query_kline(symbol = symbol_,
                                  interval = interval_,
                                  startTime = todb_,
                                  limit = limit_)
        list_data = data['result']
        insert='INSERT INTO '+ table + ' VALUES (?,?,?,?,?,?,?,?,?,?,?)'
        cursor.executemany(insert, query_kline(list_data))
        sqlite_connection.commit()
        print(f"{len(list_data)} записи успешно добавлены в таблицу")
    except sqlite3.Error as error:
        for inx, ans in enumerate(list_data):
            if ans[0] == todb_:
                print("Совпадение времени индекс: " + str(inx))
        return 2
    finally:
        if sqlite_connection:
            cursor.close()
            sqlite_connection.close()
        return 0
    
if __name__ == '__main__':
    symbol_ = ['RUNEUSDT']
    interval_ = ['1h']
##    symbol_ = ['ICXUSDT','BTCUSDT','RUNEUSDT']
##    interval_ = ['1m', '1h', '5m', '15m', '30m']
    import sys
    print(sys.argv)
    if len(sys.argv) == 3:
        symbol_ = sys.argv[1]
        interval_ = sys.argv[2]
    print("Загрузка в базу данных")
    for sym_ in symbol_:
        for in_ in interval_:
            print(f"{sym_}_{in_}")
            updateDataBybit(sym_, in_)
    print("Сохранение данных в формат pkl")
    for sym_ in symbol_:
        for in_ in interval_:
            print(f"{sym_}_{in_}")
            db2pkl(sym_, in_)    


