import requests
from enum import Enum
import logging
import json,time
from os import path

from data.index_composition import SP500_COMP

logging.getLogger().setLevel(logging.INFO)

BASE_URL="https://simfin.com/api/v1"
SIMFIN_TOKEN="4r5VV5PXW3FR3MRlAUUsXrNGuvFGpEwZ"

class PERIOD_ENUM(Enum):
    Q1="Q1"
    FY="FY"

def getSymbolId(symbol):
    id_info_url="/info/find-id/ticker/{tickerStr}".format(tickerStr=symbol)
    url = BASE_URL+id_info_url
    param ={'api-key': SIMFIN_TOKEN}

    rep = requests.get(url, params= param)
    data = rep.json()
    return data[0]['simId']

def getStatementList(symbol):
    compId = getSymbolId(symbol)
    local_url = "/companies/id/{companyId}/statements/list".format(companyId=compId)
    url = BASE_URL + local_url
    param = {'api-key': SIMFIN_TOKEN}
    rep = requests.get(url, params=param)
    data = rep.json()
    return data

def getReport(symbol,statement_type,period_enum, year):
    compId = getSymbolId(symbol)
    income_statment_url = "/companies/id/{companyId}/statements/standardised".format(companyId=compId)
    url = BASE_URL+ income_statment_url
    param = {'api-key': SIMFIN_TOKEN,'ptype':period_enum,'stype':statement_type,'fyear':year}
    rep = requests.get(url, params=param)
    data = rep.json()
    #print(data)
    return data

def getROE(symbol,period,year):
    inc_stmt = getReport(symbol,"pl", period, year)
    bal_sheet = getReport(symbol, "bs", period, year)


    total_equity = int(next((v for v in bal_sheet['values'] if v['standardisedName'] == 'Total Equity'))['valueChosen'])
    net_income = int(next((v for v in inc_stmt['values'] if v['standardisedName'] == 'Net Income'))['valueChosen'])

    return net_income/total_equity

def getROE(report_dict, symbol,period,year):
    inc_stmt = report_dict[symbol][year][period]['income_statement']
    bal_sheet = report_dict[symbol][year][period]['balance_sheet']


    total_equity = int(next((v for v in bal_sheet['values'] if v['standardisedName'] == 'Total Equity'))['valueChosen'])
    net_income = int(next((v for v in inc_stmt['values'] if v['standardisedName'] == 'Net Income'))['valueChosen'])

    return net_income/total_equity


def loadReports(symbols,quarters, years):
    report_dict={}
    for symbol in symbols:
        sym_reports = {}
        for year in years:
            y_reports={}
            for quarter in ['Q1','Q2','Q3','Q4']:
                try:
                    inc_stmt = getReport(symbol, "pl", quarter, year)
                    bal_sheet = getReport(symbol, "bs", quarter, year)
                    cash_flow = getReport(symbol, "cf", quarter, year)

                    q_reports = {'income_statement': inc_stmt,
                                 'balance_sheet':bal_sheet,
                                 'cash_flow':cash_flow}

                    y_reports[quarter]=q_reports

                    logging.info("loaded statements {symbol} {year} {period}".format(symbol=symbol,year=year, period=quarter))
                    time.sleep(0.1)

                except Exception as e:
                    logging.error("failed in getting ROE {symbol} ".format(symbol=symbol), e)

            sym_reports[year]=y_reports

        report_dict[symbol]=sym_reports



    return report_dict



if __name__ == '__main__':

    #SP500_COMP=['AAPL','LLY','MSFT','LMT']
    if not path.exists("report.txt"):
        loaded_report = loadReports(SP500_COMP,"q",[2020,2019])
        with open('report.txt', 'w') as outfile:
            json.dump(loaded_report, outfile)
        print(" loaded ", loaded_report)




    with open('report.txt') as json_file:
        reports = json.load(json_file)
    print(reports)

    roe_dict = {}
    for symbol in SP500_COMP:
        roe = getROE(reports, symbol,'Q1','2020')
        roe_dict[symbol]=roe
    print(roe_dict)
    