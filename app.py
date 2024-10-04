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

from flask import Flask

from utils.logging import logger

import requests
from concurrent.futures import ThreadPoolExecutor

from google.cloud import bigquery

# //＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝
app = Flask(__name__)

URL_GET_ADS_INSIGHTS = "https://adsanalytics-55978488217.asia-northeast1.run.app"

# //ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
def get_ad_accounts_from_bigquery():
    client = bigquery.Client()

    query = """
    SELECT *
    FROM `adsanalytics-437205.Master.ad_accounts`
    LIMIT 10
    """
    query_job = client.query(query)
    
    results = query_job.result()
    rows = [dict(row) for row in results]

    return rows

# //ーーーーーーーーーーーーーーーーーーーーー
def get_payloads():
    ad_accounts = get_ad_accounts_from_bigquery()
    payloads = []
    for account in ad_accounts:
        payload = {
            "argments":{
                "account_id":account["id"],
                "dataset_id":account["dataset_id"]
            }
        }
        payloads.append(payload)
    return payloads

# //〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜
def send_request(payload):
    response = requests.post(URL_GET_ADS_INSIGHTS,json=payload)
    return response

# //〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜
def send_requests_parallel():
    print("Parallel requests started.")

    with ThreadPoolExecutor() as executor:
        payloads = get_payloads()
        responses = executor.map(send_request, payloads)
        for res in responses:
            print(res.status_code)
            print(res.text)

    print("Parallel requests completed.")

# //ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
@app.route("/", methods=["GET","POST"])
def getPost() -> str:
    print("Process started.")
    
    results = send_requests_parallel()
    for result in results:
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
