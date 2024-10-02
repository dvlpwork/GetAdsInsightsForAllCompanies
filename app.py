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

# //＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝
app = Flask(__name__)

urls = [
    "https://ja.wikipedia.org/wiki/%E3%83%A1%E3%82%A4%E3%83%B3%E3%83%9A%E3%83%BC%E3%82%B8",
    "https://ja.wikipedia.org/wiki/%E9%98%BF%E8%B3%80%E7%A5%9E%E7%A4%BE",
    "https://ja.wikipedia.org/wiki/%E5%88%A5%E8%A1%A8%E7%A5%9E%E7%A4%BE"
]

# //ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
def send_request(url):
    response = requests.get(url)
    return response

# //〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜〜
def send_requests_parallel(urls):
    with ThreadPoolExecutor(max_workers=5) as executor:  # 最大5つのスレッドで並列実行
        executor.map(send_request, urls)

# //ーーーーーーーーーーーーーーーーーーーーー
@app.route("/", methods=["POST"])
def getPost() -> str:
    logger.info("Process started.")
    
    send_requests_parallel(urls)

    logger.info("Process completed.")
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
