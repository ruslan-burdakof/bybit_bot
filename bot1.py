from head.v1 import HeadV1, putNextPrice, putNextPrices
from exchange.bybit import Bybit
from config import api_key1, api_secret1
import numpy as np
'''
class Bot:
    def __init__(self, symbolBase, symbolQuote, head, exch):
        self.symbol = symbolBase + symbolQuote
        self.symbolBase = symbolBase
        self.symbolQuote = symbolQuote
        self.head = head
        self.exch = exch
        self.lastOrder = exch.lastExecutedOrder(symbol_)['orderId']
        self.nextOrder = self.lastOrder
    def nextStep(self):
        prices = exch.lastOpenPrices(
        if self.lastOrder == self.nextOrder:
            pass
    def uploadLastOrder(self):
        self.lastOrder = exch.lastExecutedOrder(symbol_)['orderId']
'''

import schedule
import time
from pprint import pprint
if __name__ == '__main__':
    log = open("log.txt", "w+")
    symbol_ = 'DCRUSDT'
    symbolBase = 'DCR'
    symbolQuote = 'USDT'
    exch = Bybit(api_key1, api_secret1)
    
    length_ = 60
    k_pipe_ = 0.8
    k_m_ = 1.0002
    k_price_ = 1.03
    lengthMax_ = 2*length_ - 1
    headTime, initData_ = exch.lastOpenPrices(0, symbol_, lengthMax_)
    initData = np.array(initData_)
    
    head = HeadV1(initData,
                  headTime,
                  length = length_,
                  k_pipe = k_pipe_,
                  k_m = k_m_,
                  k_price = k_price_,
                  symbol = symbol_)
    #bot = Bot(symbolBase, symbolQuote, head, exch)
    #head.bStep = False
    #head.buyPrice = 8.0268

    
    lastOrder = exch.lastExecutedOrder(symbol_)['orderId']
    nextOrder = lastOrder
    
    def uploadLastOrder():
        global lastOrder, exch
        lastOrder = exch.lastExecutedOrder(symbol_)['orderId']

    def nextStep():
        global lastOrder, head, exch, nextOrder, log, symbol_
        global symbolQuote, symbolBase
        headTime, price = exch.lastOpenPrices(head.lastTime,
                                               symbol_,
                                               lengthMax_)
        price = np.array(price)
        print(price)
        if len(price) > 1:
            head.prices = putNextPrices(head.prices, price)
            head.solve()
            price = price[-1]
            
        action = head.nextAction(price, headTime)
        print("Action: "+str(action))
        if lastOrder == nextOrder:
            if action == 1:
                exch.uploadBalance()
                qtyBye = exch.maxQtyBye(symbolBase, symbolQuote)
                orderPost = exch.buySpot(symbol_, qtyBye)
                pprint(orderPost)
                nextOrder = orderPost['orderId']
                print(nextOrder)
                lines = "Покупка: " + str(price)
                lines += "\ncacheV: "+ str(exch.balance[symbolQuote])
                lines += "\ncacheS: "+ str(exch.balance[symbolBase])
                print(lines, file = log)
                print(orderPost, file = log)
                print(lines)
                head.bStep = False
            if action == 2:
                exch.uploadBalance()
                qtySell = exch.maxQtySell(symbolBase, symbolQuote)
                orderPost = exch.sellSpot(symbol_, qtySell)
                pprint(orderPost)
                nextOrder = orderPost['orderId']
                print(nextOrder)
                lines = "Продажа: " + str(price)
                lines += "\ncacheV: "+ str(exch.balance[symbolQuote])
                lines += "\ncacheS: "+ str(exch.balance[symbolBase])
                print(lines, file = log)
                print(orderPost, file = log)
                print(lines)
                head.bStep = True


    schedule.every(10).seconds.do(uploadLastOrder)
    schedule.every().minutes.do(nextStep)
    while True:
        schedule.run_pending()
        time.sleep(1)
'''
    for pr in data:
        action =  head.nextAction(pr, 0)
        if action == 1:
            buySpot(head, pr)
        if action == 2:
            saleSpot(head, pr)
    '''
