import numpy as np
from numba import njit

# Округление вниз
@njit
def floor(val, dec):
    return float(int(val*10**dec)/10**dec)
# Rolling 1D window for ND array
@njit
def roll(a,      # ND array
         b,      # rolling 1D window array
         dx=1):  # step size (horizontal)
    shape = a.shape[:-1] + (int((a.shape[-1] - b.shape[-1]) / dx) + 1,) + b.shape
    strides = a.strides[:-1] + (a.strides[-1] * dx,) + a.strides[-1:]
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

# Добавление новой цены в массив
@njit
def putNextPrice(data, prices):
    return np.append(data[1:], prices)
@njit
def putNextPrices(data, prices):
    return np.append(data[len(prices):], prices)
    
# Создание массива изменения времени для 30, 31, ..., 60
@njit
def arange_like(data, start = 0, step = 1):
    ans = np.arange(start, int(len(data)*2), step)
    print(ans)
    return np.arange(start, int(len(data)*2), step)

    
# Получение массивов среднего и стандартных отклонений 
def getM(data, w):
    return roll(data, w).mean(axis=1)
def getStd(data, w):
    return roll(data, w).std(axis=1)
# Получение масивов полиномов для предсказания изменения среднего и стандартного отклонения 
def getCM(x, M):
    return [np.polyfit(x, M, inx) for inx in range(1,4,1)]
def getCStd(x, Std):
    return [np.polyfit(x, Std, inx) for inx in range(1,4,1)]

# Поиск полинома с лучшей экстрополяцией по последним значениям
def pInx(CM, last):
    gen = enumerate(CM)
    inx, cm = next(gen)
    N = len(last)
    last_ = np.polyval(cm, arange_like(last,N))
    m = (last - last_).std()
    for Inx, cM in gen:
        last_ = np.polyval(cM, arange_like(last,N))
        M = (last - last_).std()
        if M < m:
            inx = Inx
    return inx   

class HeadV1:
    def __init__(self,
                 initData,    # Массив цен на спот за последние length*2-1 минуты
                 initTime,    # Время инициализации
                 length = 60,
                 cacheV = 4,
                 k_pipe = 0.5,
                 k_m = 1.025,
                 k_price = 1.03,
                 symbol = "SISUSDT"):
        self.symbol = symbol
        # Ширина торгового коридора
        self.k_pipe = k_pipe
        self.k_m = k_m
        self.k_price = k_price
        # Длина окна для определения предсказания
        self.length = length
        if len(initData) == length*2-1:
            self.prices = initData
        else:
            raise Exception("Неправильная размерность входного массива")
        # Последнее время на сервере
        # Для проверки потери соединения и пропуска данных
        self.lastTime = initTime
        
        # Стоимость покупки спота       
        self.downPrice = 0
        # Объём опорной валюты
        self.cacheV = cacheV
        # Объём спотовой валюты
        self.cacheS = 0

        self._w = np.zeros(length)
        self._x = np.arange(length)

        self.solve()
        self.bStep = True
        self.buyPrice = 0
        self.salePrice = 0

    # Массив с последними значениями цен на спот
    @property
    def last(self):
        print(self.length)
        print(len(self.prices))
        print(int(self.length/2)+self.length-1)
        print(int(self.length*3/2-1))
        return self.prices[int(self.length*3/2-1):]
    # Цена продажи
    @property
    def prUp(self):
        return self.M_ + self.Std_*self.k_pipe
    # Цена покупки
    @property
    def prDown(self):
        return self.M_ - self.Std_*self.k_pipe

    def _solveM(self):
        self.M = getM(self.prices, self._w)    
    def _solveCM(self):
        self.CM = getCM(self._x, self.M)
    def _solveM_(self):
        self.pInxM = pInx(self.CM, self.last)
        self.M_ = np.polyval(self.CM[self.pInxM], self.length)

    def _solveStd(self):
        self.Std = getStd(self.prices, self._w)
    def _solveCStd(self):
        self.CStd = getCStd(self._x, self.Std)
    def _solveStd_(self):
        self.pInxStd = pInx(self.CStd, self.last)
        self.Std_ = np.polyval(self.CStd[self.pInxStd], self.length)
       
    def solve(self):
        self._solveM()
        self._solveCM()
        self._solveM_()
        self._solveStd()
        self._solveCStd()
        self._solveStd_()

    def nextAction(self, price, time):
        # 0 - ждать
        # 1 - покупать
        # 2 - продавать
        # ... - ошибка
        self.lastTime = time
        # Определили параметры для следующего предсказания
        prUp = self.prUp # При превышении цены будет проводится продажа
        prDown = self.prDown # При стоимости меньше будет проводится покупка
        # Последнее среднее и отклонение при взвешивании по центру
        M = self.M[-1] 
        Std = self.Std[-1]
        # Предсказанные среднее и отклонение на текущий момент
        M_ = self.M_ 
        Std_ = self.Std_
        action = 0
        # Проверка роста среднего
        if M_/M > self.k_m:
            action = 0
            # Проверка на выход из коридора ожидания
            if price < prDown:
                if self.bStep:
                    action = 1
                    #result = buySpot(head, price)
            if price > prUp:
                if not self.bStep and price/self.buyPrice > self.k_price:
                    action = 2
                    #result = saleSpot(head, price)
        self.prices = putNextPrice(self.prices, price)
        self.solve()
        return action
 
# Покупа спота
def buySpot(head, price):
    val = floor(head.cacheV/price, 2)
    #print(val)
    head.cacheS += val
    head.cacheV -= floor(val*price, 2)
    print("Покупка: " + str(price))
    print("cacheV: "+ str(head.cacheV))
    print("cacheS: "+ str(head.cacheS))
    head.bStep = False
    head.buyPrice = price
    return 0

def saleSpot(head, price):
    val = floor(head.cacheS, 2)
    head.cacheS -= val
    head.cacheV += floor(val*price, 2)
    print("Продажа: " + str(price))
    print("cacheV: "+ str(head.cacheV))
    print("cacheS: "+ str(head.cacheS))
    head.bStep = True
    head.salePrice = price
    return 0


if __name__ == '__main__':
    symbol_ = 'RUNEUSDT'
    interval_ = '1m'
    prt = 1
    nameFile=symbol_+ "_" + interval_ + "_prt"+str(prt) + ".pkl"
    print(nameFile)
    import pickle

    with open(nameFile, "rb") as file:
        startTime, price, highPr, lowPr, closePr, volume = pickle.load(file)
    time = np.arange(len(price))

    length_ = 25
    initTime = 0
    initData = price[:(length_*2-1)]
    len(initData)
    data = price[(length_*2-1):]
    head = HeadV1(initData,
                  initTime,
                  length = length_,
                  k_pipe = 0.8,
                  k_m = 1.0002,
                  k_price = 1.02,
                  symbol = symbol_)
    
    LL = len(price)
##    for inx, pr in enumerate(data):
##        action =  head.nextAction(pr, 0)
##        if action == 1:
##            buySpot(head, pr)
##            print(inx)
##        if action == 2:
##            saleSpot(head, pr)
##            print(inx)
##    print(inx)

