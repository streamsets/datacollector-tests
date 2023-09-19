# Copyright 2022 StreamSets Inc.
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
r"""Tools used for the revamped Oracle CDC Origin"""

from abc import ABC, abstractmethod
from contextlib import ExitStack
from datetime import datetime, timedelta
import logging
from typing import Callable

import pytest
import string

from streamsets.testframework.environment import Database
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string, Version


logger = logging.getLogger(__name__)

# These variables need to be loaded only once and will not change from test to test
SERVICE_NAME = ""  # The value will be assigned during setup
SYSTEM_IDENTIFIER = ""  # The value will be assigned during setup
DB_VERSION = 0  # The value will be assigned during setup

# Versions
FEAT_VER_FETCH_STRATEGY = Version("5.6.0")
RELEASE_VERSION = "5.4.0"
MIN_ORACLE_VERSION = 18

# Test parameters
FETCH_PARAMETERS = (
    # (fetch_strategy, fetch_overflow)
    ("DISK_QUEUE", None),
    ("MEMORY_QUEUE", None),
    ("MEMORY_OVERFLOW_DISK", -1),
    ("MEMORY_OVERFLOW_DISK", 0),
    ("MEMORY_OVERFLOW_DISK", 1),
    ("DIRECT", None),
)
RECORD_FORMATS = ["BASIC", "RICH"]


@pytest.fixture
def oracle_stage_name(sdc_builder) -> str:
    # The stage name had a type until this version
    if Version(sdc_builder.version) < Version("5.7.0"):
        return "com_streamsets_pipeline_stage_origin_jdbc_cdc_descriptiopn_OracleCDCDOrigin"
    else:
        return "com_streamsets_pipeline_stage_origin_jdbc_cdc_description_OracleCDCDOrigin"


class NoError(Exception):
    """An exception that will never be raised as it is not implemented in SDC."""
    pass


def _get_single_context_parameter(database: Database, parameter: str) -> str:
    """Retrieve the value of a context parameter from the database,
    e.g. SERVICE_NAME or INSTANCE_NAME. The parameter must have one single value."""
    with ExitStack() as exit_stack:
        logger.debug("Connect to DB")
        connection = database.engine.connect()
        exit_stack.callback(connection.close)

        query = f"SELECT SYS_CONTEXT('USERENV', '{parameter}') FROM DUAL"
        logger.debug(f"Retrieve '{parameter}' with query: {query}")
        result = connection.execute(query)
        exit_stack.callback(result.close)

        result_values = result.fetchall()
        logger.debug(f"Retrieved: {result_values}")

        assert len(result_values) == 1, f"Expected 1 {parameter} result, got '{result_values}'"
        assert len(result_values[0]) == 1, f"Expected 1 {parameter}, got '{result_values[0]}'"

        return result_values[0][0]


def _get_service_name(db: Database) -> str:
    return _get_single_context_parameter(db, "SERVICE_NAME")


def _get_system_identifier(db: Database) -> str:
    return _get_single_context_parameter(db, "INSTANCE_NAME")


def _get_database_version(db: Database) -> int:
    with ExitStack() as exit_stack:
        connection = db.engine.connect()
        exit_stack.callback(connection.close)
        db_version = connection.execute("SELECT version FROM product_component_version").fetchall()[0][0]
        str_version_list = db_version.split(".")
        version_list = [int(i) for i in str_version_list]
        return version_list[0]  # return mayor version


@pytest.fixture()
def table_name() -> str:
    """Returns a random table name"""
    return get_random_string(string.ascii_uppercase, 10)


@pytest.fixture()
def test_name(request) -> str:
    """Returns the parametrized name of the test requesting the fixture."""
    return f"{request.node.name}"


@database("oracle")
@pytest.fixture(scope="module", autouse=True)
def util_setup(database):
    """Must be imported in order to use fixtures that are dependent on this one."""
    global SERVICE_NAME, SYSTEM_IDENTIFIER, DB_VERSION

    DB_VERSION = _get_database_version(database)

    # Stop setop if the current Oracle version is not supported
    if DB_VERSION < MIN_ORACLE_VERSION:
        return

    SERVICE_NAME = _get_service_name(database)
    SYSTEM_IDENTIFIER = _get_system_identifier(database)


@pytest.fixture()
def service_name(util_setup) -> str:
    """Requires importing util_setup."""
    return SERVICE_NAME


@pytest.fixture()
def system_identifier(util_setup) -> str:
    """Requires importing util_setup."""
    return SYSTEM_IDENTIFIER


@pytest.fixture()
def database_version(util_setup) -> int:
    """Requires importing util_setup."""
    return DB_VERSION


class StartMode:
    """Class grouping together static methods that calculate start modes."""

    @staticmethod
    def current_scn(db: Database, cleanup: Callable) -> int:
        connection = db.engine.connect()
        cleanup(connection.close)
        try:
            scn = int(connection.execute("SELECT CURRENT_SCN FROM V$DATABASE").first()[0])
        except Exception as ex:
            pytest.fail(f"Could not retrieve last SCN: {ex}")
        return scn

    @staticmethod
    def future_scn(db: Database, cleanup: Callable) -> int:
        # Use a considerably greater SCN to ensure the database SCN doesn't catch up while
        # the test is running
        future_scn = StartMode.current_scn(db, cleanup) + 100
        return future_scn

    @staticmethod
    def current_instant(db: Database, cleanup: Callable) -> str:
        oracle_date_format = "YYYY-MM-DD HH24:MM:SS"

        connection = db.engine.connect()
        cleanup(connection.close)
        try:
            instant = connection.execute(f"SELECT TO_CHAR(SYSDATE, '{oracle_date_format}') FROM DUAL").first()[0]
            logger.error(instant)
        except Exception as ex:
            pytest.fail(f"Could not retrieve current database instant: {ex}")
        return instant

    @staticmethod
    def future_instant(db: Database, cleanup: Callable) -> str:
        python_date_format = "%Y-%m-%d %H:%M:%S"

        current_instant = StartMode.current_instant(db, cleanup)
        # Increase the instant by an hour
        instant = datetime.strptime(current_instant, python_date_format) + timedelta(hours=1)
        future_instant = instant.strftime(python_date_format)
        return future_instant


class Parameters(ABC):
    """A set of parameters set as Oracle CDC Origin attributes."""

    @abstractmethod
    def as_dict(self) -> dict:
        pass

    def __add__(self, other):
        """Merge two Parameters. The one on the right has preference over
        conflicting items."""
        if isinstance(other, Parameters):
            return RawParameters({**self.as_dict(), **other.as_dict()})
        return RawParameters({**self.as_dict(), **other})

    def __or__(self, other):
        """Merge two Parameters. The one on the right has preference over
        conflicting items."""
        # Starting in Python 3.9 dictionaries are merged with the OR operator, e.g. x | y
        # By using the OR operator we gain consistency between types.
        return self.__add__(other)

    def __getitem__(self, item):
        """Required to be used as kwargs with the ** operator."""
        return self.as_dict()[item]

    def keys(self):
        """Required to be used as kwargs with the ** operator."""
        return self.as_dict().keys()


class RawParameters(Parameters):
    """Empty canvas to fill with any dictionary."""

    def __init__(self, parameter_dict={}, **kwargs):
        self.parameter_dict = {**parameter_dict, **kwargs}

    def as_dict(self):
        return self.parameter_dict


class DefaultConnectionParameters(Parameters):
    """Connect via service name."""

    service_name = None

    def __init__(self, db: Database):
        self.database = db
        if self.service_name is None:
            self.service_name = _get_service_name(self.database)

    def as_dict(self):
        return {
            "host": self.database.host,
            "port": self.database.port,
            "service_name": self.service_name,
            "username": self.database.username,
            "password": self.database.password,
        }


class DefaultTableParameters(Parameters):
    """Filter a single table."""

    def __init__(self, table_name: str):
        self.table_name = table_name

    def as_dict(self):
        return {"tables_filter": [{"tablesInclusionPattern": self.table_name}]}


class DefaultStartParameters(Parameters):
    """Start with the last SCN at the moment of evaluating these parameters."""

    def __init__(self, db: Database):
        self.database = db

    def as_dict(self):
        with ExitStack() as exit_stack:
            return {
                "start_mode": "CHANGE",
                "initial_system_change_number": StartMode.current_scn(self.database, exit_stack.callback),
            }


class DefaultWaitParameters(Parameters):
    """Session wait times"""

    def as_dict(self):
        return {
            "wait_time_before_session_start_in_ms": 0,
            "wait_time_after_session_start_in_ms": 0,
            "wait_time_after_session_end_in_ms": 0,
        }
