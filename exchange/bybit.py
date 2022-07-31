from pybit.spot import HTTP
import pickle

from numba import njit
@njit
def floor(val, dec):
    return float(int(val*10**dec)/10**dec)


class Bybit:
    def __init__(self, api_key_, api_secret_):
        self.session = HTTP(endpoint = 'https://api.bybit.com',
                            api_key = api_key_,
                            api_secret = api_secret_)
        self.uploadBalance()
        self.loadPercision()
    def uploadBalance(self):
        balancePost = self.session.get_wallet_balance()
        self.balance = {}
        for coin in balancePost['result']['balances']:
            self.balance[coin['coin']] = float(coin['free'])
    def loadPercision(self):
        try:
            with open('precision.pkl', 'rb') as file:
                self.precision = pickle.load(file)
            print('load Precision')
        except Exception as e:
            print(e)
            querySymbolPost = self.session.query_symbol()['result']
            self.precision = {}
            for a in querySymbolPost:
                bprc = a['basePrecision']       # Точность спота при  продаже
                qprc = a['quotePrecision']      # Точность USDT при покупке
                mpprc = a['minPricePrecision']  # Мин. количество десятичных разрядов 
                self.precision[a['name']] = (bprc, qprc, mpprc)
            with open('precision.pkl', 'wb') as file:
                pickle.dump(self.precision, file)
    @property
    def time(self):
        return self.session.server_time()['result']['serverTime']
    
    def currentPrice(self, symbol_):
        post = self.session.last_traded_price(symbol = symbol_)['result']
        return float(post['price'])
    def lastOpenPrices(self, headTime, symbol_, lengthMax = 119):
        """
        Возвращает последнее время на сервере для интервала в одну минуту
        И массив с последними ценами по минутам
        """
        interval_ = '1m'
        serverTime = self.time
        try:
            limit_ = int(serverTime/60000) - int(headTime/60000)
        except Exception as exp:
            print(exp)
            limit_ = lengthMax
        if limit_ > lengthMax:
            limit_ = lengthMax
        if limit_ == 0:
            raise Exception("Время на сервере совпадает с входящим") 
        if limit_ < 0:
            raise Exception("Время на сервере меньше входного")
        '''
        data[0],                  #startTime [long]. Start time, unit in millisecond
        float(data[1]),           #open     [float]. Open price
        float(data[2]),           #high     [float]. High price
        float(data[3]),           #low      [float]. Low price
        float(data[4]),           #close    [float]. Close price
        float(data[5]),           #volume   [float]. Trading volume
        data[6],                  #endTime   [long]. End time, unit in millisecond
        float(data[7]),           #quoteAssetVolume [float]. Quote asset volume
        data[8],                  #trades           [int]. Number of trades
        float(data[9]),           #takerBaseVolume  [float]. Taker buy volume in base asset
        float(data[10])           #takerQuoteVolume [float]. Taker buy volume in quote asset 
        '''            
        data = self.session.query_kline(symbol = symbol_,
                                        interval = interval_,
                                        startTime = headTime,
                                        limit = limit_)
        data = data['result']
        prices = [float(d[1]) for d in data]
        print(len(data))
        return data[-1][0], prices
    def lastExecutedOrder(self, symbol_):
        orderPost = self.session.user_trade_records(symbol = symbol_, limit = 1)
        return orderPost['result'][0]
    def sellSpot(self, symbol_, qty_):
        orderPost = self.session.place_active_order(symbol = symbol_,
                                                side = "Sell",
                                                type = "MARKET",
                                                qty = qty_)
        return orderPost['result']
    def buySpot(self, symbol_, qty_):
        orderPost = self.session.place_active_order(symbol = symbol_,
                                                side = "Buy",
                                                type = "MARKET",
                                                qty = qty_)
        return orderPost['result']
    # Объём продажи максимальный
    def maxQtySell(self, symbolBase, symbolQuote):
        dec = len(self.precision[symbolBase+symbolQuote][0])-2
        qtySell = floor(self.balance[symbolBase], dec)
        return qtySell
    # Объём покупки максимальный
    def maxQtyBye(self, symbolBase, symbolQuote):
        dec = len(self.precision[symbolBase+symbolQuote][1])-2
        qtyBye = floor(self.balance[symbolQuote], dec)
        return qtyBye

if __name__ == '__main__':
    from config import api_key1, api_secret1
    from pprint import pprint
    
    exch = Bybit(api_key1, api_secret1)
    
    symbol_ = "RUNEUSDT"
    # Объём продажи максимальный
    dec = len(exch.precision['RUNEUSDT'][0])-2
    qtySell = floor(exch.balance['RUNE'], dec)
    # Объём покупки максимальный
    dec = len(exch.precision['RUNEUSDT'][1])-2
    qtyBye = floor(exch.balance['USDT'], dec)
    
##    symbol_='SISUSDT'
##    interval_ = '1m'
##    headTime = int(exch.time/60000 - 1)*60000
##    t, price = exch.lastOpenPrices(0, symbol_)
##    print(price)
##    print(t)
##    print(exch.time)
    
