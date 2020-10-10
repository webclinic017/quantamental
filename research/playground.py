import yfinance as yf
import talib
import pandas as pd

data = yf.download("FTCH, AAPL, BCEI",period='2Y',interval='1D')

tmp = data.xs('FTCH', axis=1,level=1,drop_level=True).dropna()
tmp.columns
df = pd.DataFrame()
df['Close']=tmp['Close']
df['Volume']=tmp['Volume']
df=tmp
df.tail()

(df['MACD'],df['MACD_SIG'],_)=talib.MACDEXT(df['Close'], fastperiod=12, slowperiod=26, signalperiod=9
                         ,fastmatype=1,slowmatype=1,
                         signalmatype=1)

print(df.tail())