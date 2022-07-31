#!/bin/python
from config import path_db
from pybit.spot import HTTP
from math import floor
import sqlite3

client = HTTP(endpoint='https://api.bybit.com')

PATH_DB = path_db + 'query_kline.db'
symbol_ = "SISUSDT"
interval_='1m'

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


def loadData(symbol_, interval_):
    data = client.query_kline(symbol=symbol_, interval=interval_)
    table = symbol_+'_'+interval_ 
    print("Спот_интервал: " + table +"\n")
    try:
        sqlite_connection = sqlite3.connect('query_kline.db')
        cursor  = sqlite_connection.cursor()
        print("Подключен к SQLite")
        list_data = data['result']
        insert='INSERT INTO '+table+' VALUES (?,?,?,?,?,?,?,?,?,?,?)'
        cursor.executemany(insert, query_kline(list_data))
        sqlite_connection.commit()
    except sqlite3.Error as error:
        print("Ошибка при работе с SQLite", error)
    finally:
        if sqlite_connection:
            cursor.close()
            sqlite_connection.close()
        print("Соединение с SQLite закрыто")

def lastTimeOnDB(table):
    sqlite_connection = sqlite3.connect(PATH_DB)
    cursor = sqlite_connection.cursor()
    select="select max(startTime) from " + table
    cursor.execute(select)
    return cursor.fetchone()[0]

def lastTimeOnServer():
    return client.server_time()['result']['serverTime']

def updateLatestData(symbol_, interval_):
    table = symbol_+"_"+interval_
    log = "Спот_интервал: " + table +"\n"
    try:
        sqlite_connection = sqlite3.connect(PATH_DB)
        cursor  = sqlite_connection.cursor()
        log += "Подключен к SQLite\n"
        todb_ = lastTimeOnDB(table)
        tosr_ = lastTimeOnServer()
        limit_ = floor(tosr_/60000)-floor(todb_/60000)
        data = client.query_kline(symbol = symbol_,
                                  interval = interval_,
                                  startTime = todb_,
                                  limit = limit_)
        list_data = data['result']
        insert='INSERT INTO '+ table + ' VALUES (?,?,?,?,?,?,?,?,?,?,?)'
        cursor.executemany(insert, query_kline(list_data))
        sqlite_connection.commit()
        log += str(len(list_data)) + " записи успешно вставлены ​​в таблицу\n"
    except sqlite3.Error as error:
        for inx, ans in enumerate(list_data):
            if ans[0] == todb_:
                print("Совпадение времени индекс: " + str(inx))
        print("Время в базе данных: "+str(todb_))
        print("Время на сервере: "+str(tosr_))
        print("Ошибка при работе с SQLite", error)
    finally:
        if sqlite_connection:
            cursor.close()
            sqlite_connection.close()
            log += "Соединение с SQLite закрыто"
    print(log)
    
if __name__ == '__main__':
    updateLatestData(symbol_, interval_)


