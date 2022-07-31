import numpy as np
#from numba import njit

# Rolling 1D window for ND array
#@njit
def roll(a,      # ND array
         b,      # rolling 1D window array
         dx=1):  # step size (horizontal)
    shape = a.shape[:-1] + (int((a.shape[-1] - b.shape[-1]) / dx) + 1,) + b.shape
    strides = a.strides[:-1] + (a.strides[-1] * dx,) + a.strides[-1:]
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

# Добавление новой цены в массив
#@njit
def putValueToQuery(query, value):
    return np.append(query[1:], value)


def MeanAndStd(nparray, lengthWindows):
    Mean = []
    Std = []
    start = 0
    for find in range(lengthWindows - 1, len(nparray)):
        Mean.append(nparray[start:find].mean())
        Std.append(nparray[start:find].std())
        start = start + 1
    Mean = np.array(Mean)    
    Std = np.array(Std)
    return Mean, Std
    
# Поиск полинома с лучшей экстрополяцией по последним значениям
def findBestApprMS(polMS, rangeLast, last):
    polM, polS = polMS
    lastM = np.polyval(polM[0], rangeLast)
##    print('findBestApprMS:')
##    print(len(last))
##    print(len(rangeLast))
##    print(polMS)
    inxM = 0
    sumM = ((last - lastM)**2).sum()
    for Inx in range(1, len(polM)):
        lastM = np.polyval(polM[Inx], rangeLast)
        M = ((last - lastM)**2).sum()
        if M < sumM:
            sumM = M
            inxM = Inx
    lastM = np.polyval(polM[inxM], rangeLast)
    inxS = 0
    lastS = np.polyval(polS[0], rangeLast)
    x = last - lastM
##    print(x)
    cnt = len(last)
    x68, x95 = len(x[np.abs(x)<lastS]), len(x[np.abs(x)<2*lastS])
    testS = (x68 - 0.68*cnt)**2 + (x95 - 0.95*cnt)**2
    for Inx in range(1, len(polS)):
        lastS = np.polyval(polS[Inx], rangeLast)
        x68, x95 = len(x[np.abs(x)<lastS]), len(x[np.abs(x)<2*lastS])
        test = (x68 - 0.68*cnt)**2 + (x95 - 0.95*cnt)**2
        if test < testS:
            testS = test
            inxS = Inx
    return polM[inxM], polS[inxS]

