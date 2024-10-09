from datetime import datetime, timedelta

def getToday():
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    return today_str

def getYesterday():
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    return yesterday_str