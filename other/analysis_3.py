import numpy as np
import pickle
from analys import roll, putValueToQuery, MeanAndStd, findBestApprMS

symbol_ = "RUNEUSDT"
interval_ = "1m"
prt_ = 4
# Входные данные
with open(f"{symbol_}_{interval_}_prt{prt_}.pkl", "rb")  as hole:
    startTime, openPr, highPr, lowPr, closePr, volume = pickle.load(hole)
##openPr = openPr[-168:]
# Вводные для настройки алгоритма
# Выбор интервалов времени для осреднения
length_w = 21
length_m = 2*length_w - 1

# Определение измеримого среднего и стандартного отлконения
# В настоящий момент времени
time = np.arange(0,len(openPr))
initData = openPr[:length_m]
data = openPr[length_m:]

class head:
    def __init__(self, initDataNparray):
        self.data = initDataNparray
        self.lengthW = int((len(initDataNparray)+1)/2)
        self.lengthM = len(initDataNparray)
        self._x = np.arange(self.lengthW)
        self._lengthData = len(self.data)
        self._rangeLast = np.arange(len(self.last),           # Какого хуя, так?
                                    2*(len(self.last)), 1)    # Какого хуя, так?
        self.solve()
    def _solveMS(self):
        self.M, self.S = MeanAndStd(self.data, self.lengthW)
    def _solvePolMS(self):
        polM = [np.polyfit(self._x, self.M, inx) for inx in range(1,4,1)]
        polS = [np.polyfit(self._x, self.S, inx) for inx in range(1,4,1)]
        self.polM, self.polS = findBestApprMS((polM, polS), self._rangeLast, self.last)
    def solve(self):
        self._solveMS()
        self._solvePolMS()
        self._M, self._S = np.polyval(self.polM, self.lengthW), np.polyval(self.polS, self.lengthW)
    @property
    def last(self):
        return self.data[int(self.lengthW/2)+self.lengthW-1:]   # Какого хуя, так?
    @property
    def M_(self):
        return self._M
    @property
    def S_(self):
        return self._S
    def put(self, x):
        self.data = np.append(self.data[1:], x)
        self.solve()


h = head(initData)
h.solve()
import matplotlib.pyplot as plt
aM = [h.M_]
aS = [h.S_]
for price in data:
    h.put(price)
    aM.append(h.M_)
    aS.append(h.S_)
aM = np.array(aM)
aS = np.array(aS)
t_ = time[(length_m-1):]



k_pipe = 0.8
pr = 100*((aM+k_pipe*aS)/(aM-k_pipe*aS)-1)
fig, axs = plt.subplots(2, 1)
axs[0].plot(time, openPr, label = 'price')
axs[0].plot(t_, aM, '-b',  label = 'ma_')
axs[0].plot(t_, aM-k_pipe*aS, '--r',  label = 'down')
axs[0].plot(t_, aM+k_pipe*aS, '--g', label = 'up')
axs[0].legend(loc='upper left')
axs[0].set_xlim(time[0], time[-1])
axs[0].set_ylabel(f'{symbol_}')
axs[0].set_xlabel(f'time, [{interval_}] ->')
axs[0].grid(True)

axs[1].plot(t_, pr)
axs[1].set_xlabel(f'time, [{interval_}] ->')
axs[1].set_ylabel(f'step, [%] ->')
axs[1].set_xlim(time[0], time[-1])
axs[1].grid(True)

fig.tight_layout()
plt.show()


##plt.plot(time, openPr)

##plt.plot(t_, aM-k_pipe*aS)
##plt.plot(t_, aM+k_pipe*aS)
##plt.plot(t_, pr)
##plt.show()

##
##plt.plot(t_, price)
##plt.show()
##

#Измерение времени
##import time
##t1 = time.time()
##t2 = time.time()
##print(f"time: {t2-t1}")


