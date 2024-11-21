from datetime import datetime, timedelta
import pytz


def getToday():
    jst = pytz.timezone("Asia/Tokyo")
    today = datetime.now(jst)
    today_str = today.strftime("%Y-%m-%d")
    return today_str


def getYesterday():
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)
    yesterday = now_jst - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    return yesterday_str
