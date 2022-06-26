import FinanceDataReader as fdr
import pandas as pd
import sqlite3
import pytz
import statistics

from datetime import datetime, timezone, timedelta

tickers = {"M2": "M2", "US1YT": "US1YT=X", "KOSPI": "KS11", "SPY": "SPY", "TLT": "TLT", "GLD": "GLD", "DBC": "DBC", "QQQ": "QQQ", "BTC": "BTC/KRW", "TIP":"TIP", "IAU":"IAU", "MCHI":"MCHI", "XLF":"XLF",
"VTI": "VTI", "IWM":"IWM", "VGK":"VGK", "EWJ": "EWJ", "VWO":"VWO", "GSG":"GSG", "HYG":"HYG", "LQD":"LQD", "UST":"UST", "SHV":"SHV", "USBND": "VBMFX", "GLBBND":"PGBIX", "LTBND":"VUSTX", "PSQ":"PSQ",
"USDKRW": "USD/KRW", "ASHR": "ASHR", "DBA": "DBA", "SCHD": "SCHD", "TSLA": "TSLA", "XOM": "XOM",
           "IVM": "NAESX", "IEF": "VFITX", "SHY": "SHY", "BND": "BND", "AGG": "AGG", "VCIT": "VCIT", "VNQ": "VNQ", "GUNR": "GUNR", "O": "O", "RIO": "RIO", "NAVER": "035420", "SAMSUNG": "005930"}
database = "test.db"

DEBUG_MODE = False
FEE_MODE = True
REBALANCE_PERIOD = 30


def check_table(ticker):
    """
    check_table checks whether table name same with ticker exists, or not create table and retrieve data.

    :param ticker: table_name(key in tickers list)

    :return None
    """
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (ticker,))
    table = cur.fetchone()
    if table == None:
        if DEBUG_MODE:
            print(ticker + " table not exists")
        if ticker == 'M2':
            data = fdr.DataReader('M2', data_source='fred')
        else:
            data = fdr.DataReader(tickers[ticker])
        data.to_sql(ticker, con, if_exists="replace")


def retrieve_data(ticker):
    cur.execute("SELECT Date FROM " + ticker + " ORDER BY Date DESC LIMIT 1")
    row = cur.fetchone()
    if row == None:
        if DEBUG_MODE:
            print(ticker + " table rows not exists")
        if ticker == 'M2':
            data = fdr.DataReader('M2', data_source='fred')
        else:
            data = fdr.DataReader(tickers[ticker])
        data.to_sql(ticker, con, if_exists="replace")
        cur.execute("SELECT Date FROM " + ticker +
                    " ORDER BY Date DESC LIMIT 1")
        row = cur.fetchone()
    lastdate = datetime.strptime(row[0].split(" ")[0], "%Y-%m-%d").date()

    # if(lastdate < utc_datetime.date()-timedelta(days=0)):
    if(lastdate <= local_datetime.date()):
        cur.execute("DELETE FROM " + ticker +
                    " WHERE Date >= date(?,'-0 days') ORDER BY Date DESC LIMIT 3", (lastdate,))
        con.commit()
        if ticker == 'M2':
            data = fdr.DataReader('M2', data_source='fred')
            data.to_sql(ticker, con, if_exists="replace")
        else:
            data = fdr.DataReader(
                tickers[ticker], lastdate, local_datetime.date())
            data.to_sql(ticker, con, if_exists="append")
        if DEBUG_MODE:
            print(ticker + " last Date = ", lastdate)
            cur.execute("SELECT Date FROM " + ticker +
                        " WHERE Date >= date(?,'-0 days') ORDER BY Date DESC LIMIT 3", (lastdate,))
            rows = cur.fetchall()
            for row in rows:
                print(ticker, "append Date >= lastdate ", row)


def wma(ticker, ref_date):
    """
    wma calculates ticker's moving average from ref_date with weighting 1M(12) 3M(4) 6M(2) 12M(1).

    :param ticker: table_name(key in tickers list)
    :param ref_date: datetime.date() of reference date
    :return: ticker, wma, lastdate, close_price
    """
    price = "Close"
    if ticker == 'M2':
        price = 'M2'
    cur.execute("SELECT Date, " + price + " FROM " + ticker +
                " WHERE Date <= date(?,'-0 days') ORDER BY Date DESC LIMIT 241", (ref_date, ))
    rows = cur.fetchall()
    if DEBUG_MODE:
        print(ticker, len(rows))
    m = []
    m.append(rows[0][1])
    for i in range(1, 13):
        m.append(rows[i*20][1])
    d = rows[0][0]
    weighted_moving_average = 100*(
        ((m[0]-m[1])*60+(m[0]-m[3])*20+(m[0]-m[6])*15+(m[0]-m[12])*5)/(100*m[0]))
    # MOVING AVERAGE 1Y-1M for annual rebalancing
    # ((m[0]-m[12])-(m[0]-m[1]))/m[0])
    wma = '{0:.3g}'.format(weighted_moving_average)
    if DEBUG_MODE:
        print(d.split(" ")[0], ticker + ":", m[0], "가중이평:", wma, "%")
    return ticker, wma, d.split(" ")[0], m[0]


def dual_momentum(canary_asset, portfolio, def_asset, ref_asset, ref_date):
    """
    dual_momentum calculates portfolio CAGR which invest half selected assets order from wma when ref_ticker wma is positive. Otherwise, invest in safe asset(IEF).

    :param canary_asset: canary asset which forcasts market cycle. ex) VWO, AGG, KOSPI
    :param portfolio: portfolio component tickers
    :param def_asset: defensive asset
    :param ref_date: the first day of investment
    """
    now = datetime.now().date()
    start_date = ref_date
    end_date = ref_date
    portfolio_yield = 100
    ref_yield = 100
    prev_yield = portfolio_yield
    yields = []
    prev_ref_yield = ref_yield
    ref_yields = []
    prev_selected = []
    mdd = 0
    ref_mdd = 0
    price = 'Close'
    while end_date < now:
        canary_wma = float(wma(canary_asset, end_date)[1])
        selected = []
        test_wma = {}
        for ticker in portfolio:
            if DEBUG_MODE:
                print(wma(ticker, end_date))
            test_wma[ticker] = float(wma(ticker, end_date)[1])
        sorted_test_wma = dict(
            sorted(test_wma.items(), key=lambda x: x[1], reverse=True))
        for key in sorted_test_wma:
            # RELATIVE MOMENTUM
            # if sorted_test_wma[key] > canary_wma:
            selected.append(key)
            # NUM OF HOLDING ASSETS
            if len(selected) >= len(portfolio)//2:
            # if len(selected) == len(portfolio):
                break
        # MONTHLY SEASONALITY NOV-MAY
        # if canary_wma > 0 and len(selected) >= 1 and (end_date.month > 10 or end_date.month < 6):
        if canary_wma > 0 and len(selected) >= 1:
            yields.append(0)
            for ticker in selected:
                if ticker == 'M2':
                    price = 'M2'
                cur.execute("SELECT " + price + " FROM " + ticker +
                            " WHERE Date <= date(?,'-0 days') ORDER BY Date DESC LIMIT 1", (end_date, ))
                start = float(cur.fetchone()[0])
                end_date += timedelta(days=REBALANCE_PERIOD)
                cur.execute("SELECT " + price + " FROM " + ticker +
                            " WHERE Date <= date(?,'-0 days') ORDER BY Date DESC LIMIT 1", (end_date, ))
                end = float(cur.fetchone()[0])
                price = 'Close'
                end_date -= timedelta(days=REBALANCE_PERIOD)
                # FEE SELL/BUY 0.2%
                if FEE_MODE and ticker not in prev_selected:
                    portfolio_yield *= 1 + \
                        ((end-start)/start-0.004)/len(selected)
                    yields[-1] += 100*((end-start)/start-0.004)/len(selected)
                else:
                    portfolio_yield *= 1+((end-start)/start)/len(selected)
                    yields[-1] += 100*((end-start)/start)/len(selected)
            if DEBUG_MODE:
                print(selected, end_date, "{:.2f}% {:.2f}%".format(
                    portfolio_yield, portfolio_yield-prev_yield))
            prev_selected = selected
            if portfolio_yield-prev_yield < mdd:
                mdd = portfolio_yield-prev_yield
            prev_yield = portfolio_yield
            end_date += timedelta(days=REBALANCE_PERIOD)
        else:
            if DEBUG_MODE:
                print(wma(def_asset, end_date))
            if def_asset == 'M2':
                price = 'M2'
            cur.execute("SELECT " + price + " FROM " + def_asset +
                        " WHERE Date <= date(?,'-0 days') ORDER BY Date DESC LIMIT 1", (end_date, ))
            start = float(cur.fetchone()[0])
            end_date += timedelta(days=REBALANCE_PERIOD)
            cur.execute("SELECT " + price + " FROM " + def_asset +
                        " WHERE Date <= date(?,'-0 days') ORDER BY Date DESC LIMIT 1", (end_date, ))
            end = float(cur.fetchone()[0])
            price = 'Close'
            if FEE_MODE and prev_selected != [def_asset]:
                portfolio_yield *= 1+(end-start)/start-0.004
                yields.append(100*(1+(end-start)/start-0.004))
            else:
                portfolio_yield *= 1+(end-start)/start
                yields.append(100*(1+(end-start)/start))
            if DEBUG_MODE:
                print([def_asset], end_date, start, end, "{:.2f}% {:.2f}% {:.2f}%".format(
                    100*(end-start)/start, portfolio_yield, portfolio_yield-prev_yield))
            prev_selected = [def_asset]
            if portfolio_yield-prev_yield < mdd:
                mdd = portfolio_yield-prev_yield
            prev_yield = portfolio_yield

        end_date -= timedelta(days=REBALANCE_PERIOD)
        if ref_asset == 'M2':
            price = 'M2'
        cur.execute("SELECT " + price + " FROM " + ref_asset +
                    " WHERE Date <= date(?,'-0 days') ORDER BY Date DESC LIMIT 1", (end_date, ))
        start = float(cur.fetchone()[0])
        end_date += timedelta(days=REBALANCE_PERIOD)
        cur.execute("SELECT " + price + " FROM " + ref_asset +
                    " WHERE Date <= date(?,'-0 days') ORDER BY Date DESC LIMIT 1", (end_date, ))
        end = float(cur.fetchone()[0])
        price = 'Close'
        ref_yield *= (1+(end-start)/start)
        ref_yields.append(100*(1+(end-start)/start))
        if DEBUG_MODE:
            print([ref_asset], end_date, start, end, "{:.2f}% {:.2f}% {:.2f}%".format(
                100*(end-start)/start, ref_yield, ref_yield-prev_ref_yield))
        if ref_yield-prev_ref_yield < ref_mdd:
            ref_mdd = ref_yield-prev_ref_yield
        prev_ref_yield = ref_yield

    return portfolio_yield, mdd, statistics.stdev(yields), statistics.mean(yields), ref_yield, ref_mdd, statistics.stdev(ref_yields), statistics.mean(ref_yields)


if __name__ == "__main__":
    con = sqlite3.connect(database)
    cur = con.cursor()

    # UTC TIME NOW
    native_datetime = datetime.now()
    local_time = pytz.timezone("Asia/Seoul")
    local_datetime = local_time.localize(native_datetime, is_dst=None)
    utc_datetime = local_datetime.astimezone(pytz.utc)
    if DEBUG_MODE:
        print("utc_datetime now zulu format: ", utc_datetime.strftime(
            "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")

    # RAA
    canary_asset = "KOSPI"
    portfolio = ["QQQ", "SPY", "TLT", "IEF", "IAU", "GUNR", "DBA", "USDKRW", "BTC", "SCHD", "ASHR", "PSQ", "XOM"]
    def_asset = "SHV"
    ref_asset = "M2"
    years = 3
    ref_date = (local_datetime.date()-timedelta(weeks=52*years))

    raa = []
    if canary_asset not in raa:
        raa.append(canary_asset)
    for ticker in portfolio:
        raa.append(ticker)
    if def_asset not in raa:
        raa.append(def_asset)
    if ref_asset not in raa:
        raa.append(ref_asset)
    for ticker in raa:
        check_table(ticker)
        retrieve_data(ticker)
        print(wma(ticker, local_datetime.date()))

    portfolio_yield, mdd, yield_stdev, yield_mean, ref_yield, ref_mdd, ref_yield_stdev, ref_yield_mean = dual_momentum(
        canary_asset, portfolio, def_asset, ref_asset, ref_date)
    print("[" + ref_date.strftime("%Y-%m-%d") + " ~ " +
          local_datetime.date().strftime("%Y-%m-%d") + "]")
    print("[Dual Momentum]", portfolio, "Defence", def_asset,  "CAGR {:.2f}%".format(
        100*(((1+portfolio_yield/100)/1)**(1/years)-1)), "MDD {:.2f}% yield {:.2f}% stdev {:.2f}% mean {:.4f}%".format(mdd, portfolio_yield, yield_stdev, yield_mean))
    print("|Reference|", [ref_asset], "CAGR {:.2f}%".format(
        100*(((1+ref_yield/100)/1)**(1/years)-1)), "MDD {:.2f}% yield {:.2f}% stdev {:.2f}% mean {:.4f}%".format(ref_mdd, ref_yield, ref_yield_stdev, ref_yield_mean))

    con.close()
