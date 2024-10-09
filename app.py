# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import signal
import sys
from types import FrameType

from flask import Flask,request

from utils.logging import logger

import requests
from concurrent.futures import ThreadPoolExecutor

from google.cloud import bigquery
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token

import json

import myUtil

# //＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝
app = Flask(__name__)

URL_GET_ADS_INSIGHTS = "https://adsanalytics-55978488217.asia-northeast1.run.app"

headears = {}

# //ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
def get_ad_accounts_from_bigquery():
    client = bigquery.Client()

    query = """
    SELECT *
    FROM `adsanalytics-437205.Master.ad_accounts`
    LIMIT 50
    """
    query_job = client.query(query)
    
    results = query_job.result()
    rows = [dict(row) for row in results]

    return rows

# //〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜
def init_google_authentication():
    credentials, project = google.auth.default()
    auth_req = Request()
    id_token_credential = id_token.fetch_id_token(auth_req, URL_GET_ADS_INSIGHTS)
    return id_token_credential

# //ーーーーーーーーーーーーーーーーーーーーー
def create_payloads(target_date):
    ad_accounts = get_ad_accounts_from_bigquery()
    payloads = []
    for account in ad_accounts:
        payload = {
            "arguments":{
                "account":account,
                "target_date":target_date
            }
        }
        payloads.append(payload)
    return payloads

# //〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜
def send_request(payload,headers):
    session = requests.Session()
    try:
        response = session.post(URL_GET_ADS_INSIGHTS,json=payload,headers=headers,timeout=2)
        return response
    except requests.RequestException as e:
        logger.warning("エラー検知 (タイムアウトの場合、処理は継続されます)")
        print(e)
        return None

# //〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜
def send_requests_parallel(payloads):
    print("Parallel requests started.")

    with ThreadPoolExecutor() as executor:
        headers_list = [headers for _ in range(len(payloads))]
        responses = executor.map(send_request, payloads,headers_list)

    print("Parallel requests completed.")
    return responses

# //ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
@app.route("/", methods=["GET","POST"])
def getPost() -> str:
    global headers
    print("Process started.")

    # POSTされたパラメータ整形
    params = request.get_json()
    args = json.loads(params["arguments"])
    print(f'{args} type:{type(args)}')

    # パラメータから、今日分/昨日分を判定
    if args["target_date_key"]=="today":
        target_date = myUtil.getToday()
    elif args["target_date_key"]=="yesterday":
        target_date = myUtil.getYesterday()
    else:
        target_date = myUtil.getToday()

    # get-ads-insightsにアクセスするための認証
    id_token_credential = init_google_authentication()
    headers = {
        "Authorization": f"Bearer {id_token_credential}"
    }

    # 並列でインサイトを取得
    payloads = create_payloads(target_date)
    results = send_requests_parallel(payloads)
    for result in results:
        if result is None:
            print("None")
        else:
            print(result.status_code)

    print("Process completed.")
    return "Completed."


def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")

    from utils.logging import flush

    flush()

    # Safely exit program
    sys.exit(0)


if __name__ == "__main__":
    # Running application locally, outside of a Google Cloud Environment

    # handles Ctrl-C termination
    signal.signal(signal.SIGINT, shutdown_handler)

    app.run(host="localhost", port=8080, debug=True)
else:
    # handles Cloud Run container termination
    signal.signal(signal.SIGTERM, shutdown_handler)
