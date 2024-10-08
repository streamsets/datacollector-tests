# Copyright 2024 StreamSets Inc.
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
from __future__ import annotations
from stage.utils.common import cleanup

import logging
import pytest
import socket
import string
import sqlite3
from multiprocessing import Process, Queue
from streamsets.testframework.utils import get_random_string
from time import sleep
from typing import Callable, Sequence, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime

RELEASE_VERSION = "5.10.0"
WEB_CLIENT = "Web Client"
LIBRARY = "streamsets-datacollector-webclient-impl-okhttp-lib"
logger = logging.getLogger(__name__)
ACTIVITY_DB_FILE="/tmp/mocked_endpoints_activity.db"


PER_STATUS_ACTIONS = [
    {
        "codes": ["Informational", "Successful", "Redirection"],
        "action": "Record",
        "backoff": "${unit:toMilliseconds(1, second)}",
        "retries": 5,
        "failure": "Error",
    },
    {
        "codes": ["Default", "ClientError", "ServerError"],
        "action": "ConstantRetry",
        "backoff": "${unit:toMilliseconds(1, second)}",
        "retries": 5,
        "failure": "Error",
    },
]


def _disable_flask_log():
    """Disable the Flask logger."""
    log = logging.getLogger("werkzeug")
    log.disabled = True


@pytest.fixture()
def free_port(cleanup):
    """Provides a usable port."""
    sock = socket.socket()
    sock.bind(("", 0))
    cleanup(sock.close)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    yield sock.getsockname()[1]


@pytest.fixture(scope="module")
def deps():  # x # TODO: include them in STF?
    import subprocess

    result = subprocess.run(["pip3", "install", "Flask==1.1.4", "markupsafe==2.0.1"])
    if result.returncode != 0:
        logger.error(result.stderr)


class Endpoint:
    def __init__(self, func: Callable[[], Any], methods: Sequence[str], path: Optional[str] = None, capture_activity: bool = False):
        """TODO"""
        self.__uid__ = get_random_string(string.ascii_letters, 10)
        self.__methods__ = methods
        self.__url__ = None
        self.__url_queue__ = Queue(maxsize=1)
        self.__xfunc__ = func
        self.__xpath__ = path if path is not None else f"{func.__name__}_{self.__uid__}"
        self.__capture_activity__ = capture_activity
        if self.__capture_activity__:
            self._init_db()

    def _init_db(self):
        logger.debug(f'Initializing DB context for endpoint {self.__uid__}')
        self.__db_conn__ = sqlite3.connect(ACTIVITY_DB_FILE)
        self.__db_conn__.row_factory = sqlite3.Row
        self.__db_cursor__ = self.__db_conn__.cursor()
        self.__db_table_name__ = f'endpoint_{self.__uid__}_activity'
        self.__db_cursor__.execute(f'CREATE TABLE {self.__db_table_name__} (request_verb, request_url, request_body, response, instant DATETIME DEFAULT CURRENT_TIMESTAMP)')
        logger.debug(f'Initialization done')

    @property
    def func(self) -> Callable:
        """Function to run in the endpoint."""
        def __xfunc_wrapper__():
            from flask import request
            r = self.__xfunc__()

            _local_db_conn = sqlite3.connect(ACTIVITY_DB_FILE)
            _local_db_conn.cursor().execute(f'INSERT INTO {self.__db_table_name__} (request_verb, request_url, request_body, response) VALUES ("{request.method}", "{request.full_path}", "{request.get_data(as_text=True)}", "{r}")')
            _local_db_conn.commit()
            _local_db_conn.close()
            return r
        return self.__xfunc__ if not self.__capture_activity__ else __xfunc_wrapper__

    @property
    def methods(self) -> Sequence[str]:
        """REST methods accepted by the endpoint."""
        return self.__methods__

    @property
    def path(self) -> str:
        """Path to the endpoint."""
        return self.__xpath__

    @property
    def url(self) -> Optional[str]:
        """Full url of the endpoint. It has a null value until @Endpoint.recv_url runs at least once.
        Should only be used once."""
        return self.__url__

    def send_url(self, url) -> None:
        """Send the url to the queue. Used to communicate it between different processes."""
        self.__url_queue__.put(url, block=False)

    def recv_url(self) -> Optional[str]:
        """Receive the url from the queue. Used to communicate it between different processes.
        Should only be called once, after that @Endpoint.url should be called."""
        self.__url__ = self.__url_queue__.get()
        return self.url

    @property
    def activity(self) -> EndpointActivity:
        if not self.__capture_activity__:
            raise NoActivityCaptured('No activity has been captured. To capture activity please use capture_activity=True')
        activity = self.__db_cursor__.execute(f'SELECT * from {self.__db_table_name__} ORDER BY INSTANT ASC').fetchall()
        activity_items = [EndpointActivityItem(**dict(a)) for a in activity]
        return EndpointActivity(hits=len(activity_items), activity_items=activity_items)

    def __del__(self):
        if self.__capture_activity__:
            self.__db_cursor__.execute(f'DROP TABLE {self.__db_table_name__}')
            self.__db_cursor__.close()
            self.__db_conn__.close()

class NoActivityCaptured(Exception):
    pass

@dataclass
class EndpointActivity:
    hits: int
    activity_items: List[EndpointActivityItem]

@dataclass
class EndpointActivityItem:
    request_verb: str
    request_url: str
    request_body: str
    response: str
    instant: str

class Server:
    def __init__(self, addr: str, port: int):
        """TODO"""
        self.__id__ = get_random_string(string.ascii_letters, 10)
        self.__proto__ = "http"
        self.__addr__ = addr
        self.__port__ = port
        self.__hostname__ = socket.gethostname()
        self.__process__ = None
        self.__ready_queue__ = Queue(maxsize=1)

    @property
    def id(self) -> str:
        """Unique string identifying the server."""
        return self.__id__

    @property
    def url(self):
        """Base URL of the server."""
        return f"{self.__proto__}://{self.__hostname__}.cluster:{self.__port__}"

    @property
    def port(self):
        """Port of the server."""
        return self.__port__

    def notify_ready(self) -> None:
        """Send a notification to server.ready()"""
        sleep(0.5)
        self.__ready_queue__.put(True, block=False)

    def ready(self) -> None:
        """Wait to be notified by server.notify_ready()."""
        self.__ready_queue__.get()

    def start(self, endpoints: Sequence[Endpoint]) -> None:
        """Run a server on a different process."""
        self.__process__ = Process(target=self.run, args=[endpoints])
        self.__process__.start()

    def run(self, endpoints: Sequence[Endpoint]) -> None:
        """Create and run a Flask process with the specified function in a specific path."""
        from flask import Flask  # x

        _disable_flask_log()

        app = Flask(self.id)

        for endpoint in endpoints:
            app.add_url_rule(f"/{endpoint.path}", endpoint.path, endpoint.func, methods=endpoint.methods)
            url = f"{self.__proto__}://{self.__hostname__}.cluster:{self.__port__}/{endpoint.path}"
            endpoint.send_url(url)

        self.notify_ready()
        app.run(host=self.__addr__, port=self.__port__)

    def stop(self) -> None:
        """Stop the process running the server."""
        if self.__process__ is not None:
            self.__process__.terminate()
            self.__process__.join()


@pytest.fixture()
def server(free_port: int, deps) -> Server:  # x, deps ensures Flask is installed
    """TODO"""
    return Server("0.0.0.0", free_port)


def verify_header():
    """
    Util Method - Verify Header used in tests for WebClient Origin, Processor and Destination
    """

    from flask import request, json

    success_message = 'Success! headers are present'
    failure_message = 'Missing header "header1"'
    failure_message2 = 'Missing header "header2"'

    # Check if the 'header1' header is present in the request
    if 'header1' not in request.headers:
        return json.dumps(failure_message)
    # Check if the 'header2' header is present in the request
    if 'header2' not in request.headers:
        return json.dumps(failure_message2)

    # If 'header1' and 'header2' header are present, return success response
    return json.dumps(success_message)
