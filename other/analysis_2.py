import pandas as pn
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
from matplotlib.widgets import Slider, Button

    # Входные данные
# Загрузка из базы данных
con = sqlite3.connect('query_kline.db')
data = pn.read_sql('''select * from SISUSDT_1m ORDER BY startTime''',con)
data.startTime = pn.to_datetime(data.startTime, unit = 'ms')
# Выбор диапазона времени
t1 = pn.to_datetime('2022-02-21 16:53:00')
t2 = pn.to_datetime('2022-03-10 12:00:00')
data = data[(data.startTime>=t1)&(data.startTime<=t2)]

    #  Вводные для настройки алгоритма
# Выбор времени осреднения в минутах
length_of_time=120
# Коэффициент тунеля
k_pipe = 0.8

# Определение измеримого среднего и стандартного отлконения
# В настоящий момент времени
Mr = data.open.rolling(window=length_of_time).mean()
Sr = data.open.rolling(window=length_of_time).std(ddof=0)
Mr_up = Mr+Sr
Mr_down = Mr-Sr
Mr_up.name = 'Mr_up'
Mr_down.name = 'Mr_down'
Mr.name, Sr.name = 'Mr', 'Sr'
# Определение среднего и стандартного отлконения
# В настоящий момент времени
M = data.open.rolling(window=length_of_time, center = True).mean()
S = data.open.rolling(window=length_of_time, center = True).std(ddof=0)
M_down = M-S*k_pipe
M_up = M+S*k_pipe
M_up.name = 'M_up'
M_down.name = 'M_down'
M.name, S.name = 'M', 'S'
# Отсев данных по среднему и отклонениям
data_pipe = data.open
data_pipe.name ='pipe'
data_max = data_pipe[data_pipe>=M_up]
data_max.name = 'max'
data_min = data_pipe[data_pipe<=M_down]
data_min.name = 'min'
def findLengthExtr(series):
    start = series.diff()
    inx_s = start[start == 1].index
    end = series.diff(periods=-1)
    inx_e = end[end == 1].index
    return pn.Series(inx_e - inx_s + 1, index = inx_s)

series_max = pn.Series(0, index = data_pipe.index)
series_max[data_max.index] = 1
series_min = pn.Series(0, index = data_pipe.index)
series_min[data_min.index] = 1

lengthMax = findLengthExtr(series_max)
lengthMin = findLengthExtr(series_min)

def findSteps(aInxMin, aInxMax):
    steps = []
    genInxMax = iter(aInxMax)
    for inxMin in aInxMin:
        a = inxMin
        for b in genInxMax:
            if b > a:
                break
        steps.append((a,b))
    return steps

aInxMin = lengthMin.index
aInxMax = lengthMax.index
steps = findSteps(aInxMin, aInxMax)
inxStep = pn.DataFrame(steps)
stepMin,stepMax = inxStep[0], inxStep[1]

k_exchange = 0.01

k_step=data.open[stepMax].array/data.open[stepMin]
k_step.name = 'k_step'

k = k_step.prod()*(1 - k_exchange)**2*len(steps)
'''
M_pipe = pn.concat([data.startTime, data_pipe, k_step], axis = 1)
M_pipe.plot(x = "startTime",
            y = ['pipe', 'k_step'],
            style = ['--y','-g'],
            linewidth = 0.8)

# при обработке боллшинства шагов
aa = k_step[k_step > 1.02]
print('Максимальная сумма выигрыша')
print((aa*0.99*0.99).prod())

# 

M_pipe = pn.concat([data.startTime, data_pipe, data_max, data_min], axis = 1)
M_pipe.plot(x="startTime",
            y = ['pipe', 'max', 'min', 'k_step'],
            style = ['--y','-g', '-r', '-b'],
            linewidth = 0.8)
'''
# MM = pn.concat([data.startTime,M,M_up,M_down, data.open],axis=1)
# MM.plot(x="startTime", y = ['M','M_up', 'M_down', 'open'])

'''
# Построение графика изменения 
MM = pn.concat([data.startTime,M,M_up,M_down, data.open],axis=1)
MMr = pn.concat([data.startTime,Mr,Mr_up,Mr_down, data.open],axis=1)
MM.plot(x="startTime", y = ['M','M_up', 'M_down', 'open'])
#MMr.plot(x="startTime", y = ['Mr','Mr_up', 'Mr_down', 'open'])
plt.show()
plt.show()

m = M.dropna()
inx = m.index
d=(data.open[inx]-m)
#m.diff().plot()
#(data.close[inx]-data.open[inx]).plot()

'''
ser = pn.Series(0, index = data_pipe.index)
ser[lengthMax.index] = 1
freq_max = ser.rolling(window=1440, center = True).mean()
#freq_max = ser.rolling(window=1440, center = True).mean()
freq_max.plot()
plt.show()
