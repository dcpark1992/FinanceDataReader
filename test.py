import FinanceDataReader as fdr
import pandas as pd
import sqlite3
import pytz

from datetime import datetime, timezone, timedelta

ticker = {"KOSPI": "KS11", "SPY": "VFINX", "TLT": "VUSTX", "GLD": "GLD", "DBC": "DBC", "IVM": "NAESX", "IEF": "VFITX", "SHY": "VFISX", "NAVER": "035420", "SAMSUNG": "005930"}
database = "test.db"

con = sqlite3.connect(database)
cur = con.cursor()

# UTC TIME NOW
native_datetime = datetime.now()
local_time = pytz.timezone("Asia/Seoul")
local_datetime = local_time.localize(native_datetime, is_dst=None)
utc_datetime = local_datetime.astimezone(pytz.utc)
print("utc_datetime now zulu format: ", utc_datetime.strftime(
    "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")

# UPDATE TABLE
tables = []
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
for row in cur.fetchall():
    tables.append(row[0])
for table in tables:
    if table not in ticker:
        cur.execute("DROP TABLE IF EXISTS "+table)
        con.commit()
        tables.remove(table)
print("tables: ", tables)

for k in ticker.keys():
    if k in tables:
        cur.execute("SELECT Date FROM " + k + " ORDER BY Date DESC LIMIT 1")
        row = cur.fetchone()
        lastdate = datetime.strptime(row[0].split(" ")[0], "%Y-%m-%d").date()
        if(lastdate < utc_datetime.date()-timedelta(days=1)):
            print(k + " table last update date: ", lastdate)
            data = fdr.DataReader(
                ticker[k], lastdate+timedelta(days=1), utc_datetime.date()-timedelta(days=1))
            data.to_sql(k, con, if_exists="append")
    else:
        print(k + " table not exists")
        data = fdr.DataReader(ticker[k])
        data.to_sql(k, con, if_exists="replace")

    # WEIGHTED MOVING AVERAGE
    cur.execute("SELECT Close FROM " + k + " ORDER BY Date DESC LIMIT 250")
    rows = cur.fetchall()
    m = []
    m.append(rows[1][0])
    for i in range(1,13):
        m.append(rows[i*20][0])
    weighted_moving_average = ((m[0]-m[1])*12+(m[0]-m[3])*4+(m[0]-m[6])*2+(m[0]-m[12]))*100/(18*m[0])
    wma = '{0:.3g}'.format(weighted_moving_average)

    print(k + ":", m[0], "가중이동평균(1/3/6/12M):", wma, "%")

con.close()