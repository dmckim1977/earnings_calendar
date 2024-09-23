import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2 as pg
import requests
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_earnings_calendar(earnings_date):
    query = """ 
Select * 
FROM earnings_calendar_detail
WHERE date = %(today)s;"""

    conn = pg.connect(
        user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT, database=DB_NAME
    )

    df = pd.read_sql(query, conn, params={"today": earnings_date})

    return df


def get_earnings_data(earnings_date):
    all = get_earnings_calendar(earnings_date)
    all["time_index"] = pd.to_datetime(all["time"], format="%H:%M:%S")
    all.set_index("time_index", inplace=True)
    am = all.between_time("00:01", "12:00")
    pm = all.between_time("12:00", "23:59")
    return am, pm


def rank_and_sort_earnings(df):
    df.sort_values("importance", inplace=True, ascending=False)
    high_importance = df[df["importance"] > 4]
    if len(high_importance) > 8:
        print("There are more than 8 companies reporting over Importance of 5")

    # Count the rows in the dataframe
    df_count = len(df)
    if df_count <= 8:
        tickers_list = df["symbol"].to_list()
        others_reporting = None
        return tickers_list, others_reporting
    else:
        tickers_list = df["symbol"][:8].to_list()
        others_reporting = df_count - 8
        return tickers_list, others_reporting


def format_earnings_string(tickers_list, others_reporting, reporting_time: str = "am"):
    if len(tickers_list) == 0:
        if reporting_time == "am":
            return "There are no notable earnings before the bell."
        elif reporting_time == "pm":
            return "There are no notable earnings after the bell."

    if reporting_time == "am":
        earnings_str = "<h6>Before the Bell: "
    else:
        earnings_str = "<h6>After the Bell: "

    for ticker in tickers_list:
        earnings_str += ticker + ", "
    if others_reporting:
        earnings_str += f"and {others_reporting}"
    else:
        earnings_str = earnings_str[:-2]

    earnings_str += "</h6>"
    return earnings_str.strip().replace("\n", "").replace("\t", "")


def get_earnings(earnings_date):
    am, pm = get_earnings_from_stocktwits_api(publish_date=earnings_date)
    # am, pm = get_earnings_data(earnings_date)
    am_list, am_others = rank_and_sort_earnings(am)
    pm_list, pm_others = rank_and_sort_earnings(pm)
    am_str = format_earnings_string(am_list, am_others, reporting_time="am")
    pm_str = format_earnings_string(pm_list, pm_others, reporting_time="pm")
    return am_str, pm_str


def format_earnings_html(
    am_tickers_list, am_others_reporting, pm_tickers_list, pm_others_reporting
):
    am_str = None
    am_earnings_str = ""
    pm_str = None
    pm_earnings_str = ""

    if not am_tickers_list:
        am_str = "There are no notable earnings before the bell."
    if not pm_tickers_list:
        pm_str = "There are no notable earnings after the bell."

    if not am_str:
        for ticker in am_tickers_list:
            am_earnings_str += ticker + ", "
        if am_others_reporting:
            am_earnings_str += f"and {am_others_reporting} others reporting"
        else:
            am_earnings_str = am_earnings_str[:-2]

    if not pm_str:
        for ticker in pm_tickers_list:
            pm_earnings_str += ticker + ", "
        if pm_others_reporting:
            pm_earnings_str += f"and {pm_others_reporting} others reporting"
        else:
            pm_earnings_str = pm_earnings_str[:-2]

    return am_earnings_str, pm_earnings_str


def newsletter_earnings(earnings_date):
    am, pm = get_earnings_from_stocktwits_api(publish_date=earnings_date)
    am_list, am_others = rank_and_sort_earnings(am)
    pm_list, pm_others = rank_and_sort_earnings(pm)
    am_earnings_str, pm_earnings_str = format_earnings_html(
        am_list, am_others, pm_list, pm_others
    )
    return am_earnings_str, pm_earnings_str


def stocktwits_earnings_dates(publish_date):
    end_date = datetime.strptime(publish_date, "%Y-%m-%d") + timedelta(days=1)
    end_str = end_date.strftime("%Y-%m-%d")
    return publish_date, end_str


def get_stocktwits_earnings(start, end):
    url = f"https://api.stocktwits.com/api/2/discover/earnings_calendar?date_from={start}&date_to={end}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }

    res = requests.get(url, headers=headers)

    return res.json()


def filter_by_date(js, date):
    df = pd.DataFrame(js["earnings"][date]["stocks"])
    return df


def split_by_time(df):
    print(df)
    pm = df[df["time"] > "12:00:00"]
    am = df[df["time"] < "12:00:00"]
    return am, pm


def get_earnings_from_stocktwits_api(publish_date):
    dates = stocktwits_earnings_dates(publish_date)
    js = get_stocktwits_earnings(dates[0], dates[1])
    df = filter_by_date(js, publish_date)
    am, pm = split_by_time(df)
    return am, pm


if __name__ == "__main__":
    # am, pm = newsletter_earnings(datetime.today().date())
    # print(am, pm)

    publish_date = "2024-07-24"
    test = newsletter_earnings(publish_date)
    print(test)
