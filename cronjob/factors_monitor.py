from factors import factory as fty
from data.symbollist import CUS_SPY_SYMBOLS
from utils import emailhelper

import yfinance as yf

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from spac.config import Secret,Config
import os
CWD  = os.path.dirname(os.path.realpath(__file__))+"/"


def generate_super_trend_graph(org_df, super_trend_line, cross_up, cross_dn, filename="graph.html"):
    file=open(CWD+filename,'w+')
    for i in range(2):
        side_sig = [cross_up, cross_dn][i]
        side = "BUY" if i == 0 else "SELL"
        file.write("<h1> {0}</h1>  ".format(side))
        #print(side)

        for item in side_sig[side_sig == True].iloc[-1].dropna().iteritems():
            #print("item",item)
            symbol = item[0]
            print("%s: %s"%(side,symbol))
            file.write("<h2> %s: %s</h2>"%(side,symbol))
            buy = org_df['Close', symbol][cross_up[symbol] == True]
            sell = org_df['Close', symbol][cross_dn[symbol] == True]

            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            # Add traces
            fig.add_trace(go.Scatter(x=org_df.index, y=org_df['Close', symbol], name=symbol), secondary_y=False)
            fig.add_trace(go.Scatter(x=org_df.index, y=super_trend_line[symbol][super_trend_line[symbol]!=0], name='st_line'),
                          secondary_y=False)

            fig.add_trace(go.Scatter(x=buy.index, y=buy, name='buy'), secondary_y=False)
            fig.add_trace(go.Scatter(x=sell.index, y=sell, name='sell'), secondary_y=False)
            #fig.show()

            file.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))

    file.close
    return filename


def send_email(filename):
    pass

if __name__ == '__main__':


    symbols = CUS_SPY_SYMBOLS
    data = yf.download(symbols, period='2Y', interval='1d')

    df = data.copy()

    #df = append_return_cols(df, 'Close', 1, 1)
    #df = append_return_cols(df, 'Close', 5, 5)
    #df = append_return_cols(df, 'Close', 5, 30)
    #df = append_return_cols(df, 'Volume', 5, 10)
    #df = append_return_cols(df, 'Volume', 5, 30)
    #df = append_macd_cols(df, 'Close')
    #df = append_bollinger_band_cols(df, 'Close')
    #df = append_rsi_cols(df, 'Close')
    #df = append_fwd_return_cols(df, 'Close', 5, 30)

    cross_up,cross_dn,st_line,df =fty.super_trend_vpt(df)


    file = generate_super_trend_graph(df, st_line, cross_up, cross_dn)
    emailhelper.send_email("Super Trend Watch List","", [file], CWD)