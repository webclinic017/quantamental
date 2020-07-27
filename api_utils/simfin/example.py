import simfin as sf
import pandas as pd
import logging

from data.index_composition import SP500_COMP

#SIMFIN_TOKEN="4r5VV5PXW3FR3MRlAUUsXrNGuvFGpEwZ"

#sf.set_api_key(SIMFIN_TOKEN)

# Set the local directory where data-files are stored.
# The dir will be created if it does not already exist.
sf.set_data_dir('./simfin_data/')

# Load the annual Income Statements for all companies in USA.
# The data is automatically downloaded if you don't have it already.
df_income = sf.load_income(variant='quarterly', market='us')
df_balance = sf.load_balance(variant='quarterly', market='us')
#print(df_income.loc['MSFT'].tail())
print(df_balance.index)

symbols= df_balance.index.levels[0]
print(symbols)


#
# roe ={}
df = pd.DataFrame()
# for sym in ['A', 'AA', 'AAC', 'AAL', 'AAMC', 'AAN', 'AAOI', 'AAON', 'AAP', 'AAPL']:
#     try:
#
#         net_income = df_income.loc[sym]['Net Income']
#         total_equity = df_balance.loc[sym]['Total Equity']
#         print(" getttin report nuber ", sym)
#         df[sym]=net_income/total_equity
#         print( (net_income/total_equity).tail())
#     except Exception as e:
#         logging.error("error symbol {symbol}".format(symbol=sym))
#
# #print(df.rank(1, ascending=False,).dropna())
# print(df.tail())


net_income = df_income[ (df_income['Fiscal Year']==2019) & (df_income['Fiscal Period']=='Q1')]['Net Income']
print(net_income)

total_equity = df_balance[ (df_balance['Fiscal Year']==2019) & (df_balance['Fiscal Period']=='Q1')]['Total Equity']
print(total_equity)

roe=net_income/total_equity


print(roe.sort_values())

publish_date= df_income[(df_income['Fiscal Year']==2019) & (df_income['Fiscal Period']=='Q1')]['Publish Date']
print( publish_date)