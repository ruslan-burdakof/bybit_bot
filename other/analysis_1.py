import pandas as pn
import sqlite3
import matplotlib.pyplot as plt
con = sqlite3.connect('query_kline.db')
data = pn.read_sql('''select * from SISUSDT_1m ORDER BY startTime''',con)
data.startTime = pn.to_datetime(data.startTime, unit = 'ms')
#data.endTime = pn.to_datetime(data.endTime, unit = 'ms')
#print(data.head(5).T)
t1 = pn.to_datetime('2022-02-21 16:53:00')
t2 = pn.to_datetime('2022-03-10 12:00:00')
data = data[(data.startTime>=t1)&(data.startTime<=t2)]

length_of_time=120
# Определение измеримого среднего и стандартного отлконения
# В настоящий момент времени
Mr = data.open.rolling(window=length_of_time).mean()
Sr = data.open.rolling(window=length_of_time).std(ddof=0)
Mr_up = Mr+Sr
Mr_down = Mr-Sr

Mr_up.name = 'Mr_up'
Mr_down.name = 'Mr_down'
Mr.name, Sr.name = 'Mr', 'Sr'
# Определение истинного среднего и стандартного отлконения
# В настоящий момент времени
M = data.open.rolling(window=length_of_time, center = True).mean()
S = data.open.rolling(window=length_of_time, center = True).std(ddof=0)
M_down = M-S
M_up = M+S
M_up.name = 'M_up'
M_down.name = 'M_down'
M.name, S.name = 'M', 'S'




#MM = pn.concat([data.startTime,M,Mr, data.open, S, Sr,V],axis=1)
MM = pn.concat([data.startTime,M,M_up,M_down, data.open],axis=1)
MMr = pn.concat([data.startTime,Mr,Mr_up,Mr_down, data.open],axis=1)
m = M.dropna()
inx = m.index
MM.plot(x="startTime", y = ['M','M_up', 'M_down', 'open'])
#MMr.plot(x="startTime", y = ['Mr','Mr_up', 'Mr_down', 'open'])
d=(data.open[inx]-m)
#m.diff().plot()
#(data.close[inx]-data.open[inx]).plot()
plt.show()


'''
op = data.open
c = data.close

df=(c-op).rolling(window=30).mean()
op.plot()
(500*df).plot()
plt.show()
'''
