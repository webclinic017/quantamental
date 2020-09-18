import pandas as pd
import yfinance as yf

import time
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE

from spac.config import Secret,Config


def load_spac_info(path_to_csv="active_spacs_clean.csv"):
    df = pd.read_csv(path_to_csv)
    return df


def load_hist_data(tickers, start=None, end=None, interval='1d', valid_data_window=10):
    price_df = None
    if start is None or end is None:
        price_df = yf.download(tickers, period='1Y', interval=interval)
    else:
        price_df = yf.download(tickers, start, end, interval=interval)

    invalid_tickers = price_df['Close'].iloc[-valid_data_window - 1].isnull()
    invalid_tickers = invalid_tickers[invalid_tickers == True]

    price_df = price_df.drop(invalid_tickers.index.tolist(), axis=1, level=1)

    return price_df


'''
return dataframe columns name like "[('col_1', 'ticker_1'),('col_1','ticker_2'),...]"
'''


def gen_factor_1_df(price_df, close_col='Close', volume_col='Volume', look_back=10, ma_window=10):
    df = pd.DataFrame(index=price_df.index)

    rolling_mean = price_df.xs(close_col, axis=1, level=0, drop_level=False).rolling(window=ma_window).mean()
    return_cols = rolling_mean / rolling_mean.shift(look_back) - 1
    return_cols = return_cols.rename(columns={close_col: 'price_chg_pct'})
    df = df.join(return_cols)
    # print(df.columns)

    vol_rolling_mean = price_df.xs(volume_col, axis=1, level=0, drop_level=False).rolling(window=ma_window).mean()
    vol_chg_pct = vol_rolling_mean / vol_rolling_mean.shift(look_back) - 1
    vol_chg_pct = vol_chg_pct.rename(columns={volume_col: 'vol_chg_pct'})
    df = df.join(vol_chg_pct)

    avg_price = price_df[['Close', 'Open', 'High', 'Low']].mean(axis=1, level=1)
    volume = price_df['Volume']
    volume_in_usd = avg_price * volume
    # print(volume_in_usd.columns,avg_price.columns )
    for col_name in volume_in_usd.columns:
        volume_in_usd = volume_in_usd.rename(columns={col_name: ('volume_in_usd', col_name)})
    df = df.join(volume_in_usd)

    return df


def get_f1_tickers(tickers, f1_df):
    abnormal_tickers = []
    for ticker in tickers:
        is_abnormal_return = f1_df[('price_chg_pct', ticker)].iloc[-1] > Config.price_abnormal
        is_abnormal_volume = f1_df[('vol_chg_pct', ticker)].iloc[-1] > Config.volume_abnormal
        if is_abnormal_return and is_abnormal_volume:
            # print( ticker, is_abnormal_return)
            abnormal_tickers.append(ticker)

    return abnormal_tickers

def gen_f1_to_send(price,filename='abnormal_spac.txt'):
    df = gen_factor_1_df(price)
    df.tail()
    abnormal_tickers = get_f1_tickers(price['Close'].columns, df)

    to_send = {}
    for ticker in abnormal_tickers:

        tmp = pd.DataFrame()
        to_send[ticker] = tmp
        # tmp = df[[('price_chg_pct',ticker), ('vol_chg_pct',ticker),('volume_in_usd',ticker)]]
        tmp['price change(%)'] = (df[('price_chg_pct', ticker)] * 100).apply(lambda x: '{:,.2f}%'.format(x))
        tmp['volume change(%)'] = (df[('vol_chg_pct', ticker)] * 100).apply(lambda x: '{:,.2f}%'.format(x))
        tmp['volume USD($MM)'] = (df[('volume_in_usd', ticker)] / 1000000).apply(lambda x: '${:,.2f}MM'.format(x))
        tmp['Close'] = price['Close'][ticker].apply(lambda x: '{:,.2f}'.format(x))

        # has_target = (spac_info[spac_info['Symbol']==ticker]['Merger Target?']).values
        # ipo_date = spac_info[spac_info['Symbol']==ticker]['IPO Date'].values
        liq_date = spac_info[spac_info['Symbol'] == ticker][
            ['Symbol', 'Merger Target?', 'IPO Date', 'Liquidation Date']]

    #filename = 'sample.txt'
    file_full_name = './' + filename
    with open(file_full_name, 'w') as outfile:
        for ticker, df in to_send.items():
            outfile.write('\n======== %s ========\n' % (ticker))
            header = spac_info[spac_info['Symbol'] == ticker][
                ['Symbol', 'Merger Target?', 'IPO Date', 'Liquidation Date']]
            header.to_string(outfile)
            outfile.write('\n')
            to_send[ticker].tail(10).to_string(outfile)
            outfile.write('\n\n')

    #return to_send

def send_email(filename = 'abnormal_spac.txt', path_to_file='./'):
    try:
        file_full_name = path_to_file+filename
        server = smtplib.SMTP("smtp.gmail.com", "587")
        # self.server.ehlo() # Can be omitted
        server.starttls()
        # self.server.ehlo() # Can be omitted
        server.login(Secret.EMAILFROM,Secret.EMAILPW)

        message = MIMEMultipart()
        message['From'] = Secret.EMAILFROM
        message['Bcc'] = COMMASPACE.join(Secret.EMAILDICT['bcc'])
        message['Subject'] = filename

        sent = 0
        retry = 0
        while sent == 0:
            try:

                with open(file_full_name, "rb") as attachment:
                    # Add file as application/octet-stream
                    # Email client can usually download this automatically as attachment
                    part = MIMEApplication(
                        attachment.read(),
                        Name=filename
                    )

                # Add header as key/value pair to attachment part
                part['Content-Disposition'] = 'attachment; filename="%s"' % (filename)

                # Add attachment to message and convert message to string
                message.attach(part)
                text = message.as_string()

                server.sendmail(Secret.EMAILFROM, Secret.EMAILDICT['bcc'], text)
                sent += 1
            except Exception as e:
                time.sleep(1)  # TODO not good
                retry += 1
                if retry == 2:
                    raise Exception("Failed to send Email after retry %s" % e)
                    break
    except Exception as e:
        print('Exception', e)
    finally:
        server.quit()

if __name__ == '__main__':
    spac_info = load_spac_info("/Users/ZhenxinLei/MyWork/quantamental/spac/active_spacs_clean.csv")
    tickers = list(spac_info['Symbol'])
    price = load_hist_data(tickers)
    gen_f1_to_send(price)
    send_email()