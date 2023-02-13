# Copyright 2023 StreamSets Inc.
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

import logging
import time

import pytest
import sqlalchemy

from contextlib import ExitStack

from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string


ORACLE_CDC = "Oracle CDC"
RECORD_COUNT = 10**7
RELEASE_VERSION = "5.4.0"
pytestmark = [database("oracle"), sdc_min_version(RELEASE_VERSION)]


# TODO replace credentials fixture with .configure_for_environment once it is ready
@database("oracle")
def default_config(db):
    database = db
    service_name = "ORCLCDB"  # default value
    start_scn = 0  # default value

    with ExitStack() as exit_stack:
        parameter = "SERVICE_NAME"
        connection = database.engine.connect()
        exit_stack.callback(connection.close)

        query = f"SELECT SYS_CONTEXT('USERENV', '{parameter}') FROM DUAL"
        result = connection.execute(query)
        exit_stack.callback(result.close)

        # obtain service name
        result_values = result.fetchall()
        assert len(result_values) == 1, f"Expected 1 {parameter} result, got '{result_values}'"
        assert len(result_values[0]) == 1, f"Expected 1 {parameter}, got '{result_values[0]}'"
        service_name = result_values[0][0]

        # obtain last scn
        try:
            start_scn = int(connection.execute("SELECT CURRENT_SCN FROM V$DATABASE").first()[0])
        except Exception as ex:
            pytest.fail(f"Could not retrieve last SCN: {ex}")

    return {
        "host": database.host,
        "port": database.port,
        "service_name": service_name,
        "username": database.username,
        "password": database.password,
        "start_mode": "CHANGE",
        "initial_system_change_number": start_scn,
    }


@pytest.mark.parametrize("batch_size", [1_000, 10_000, 20_000])
def test_defaults(sdc_builder, sdc_executor, database, origin_table, batch_size):

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    oracle_cdc = pipeline_builder.add_stage(ORACLE_CDC)
    default_parameters = default_config(database)
    oracle_cdc.set_attributes(tables_filter=[{"tablesInclusionPattern": origin_table.name}], **default_parameters)

    oracle_cdc >> benchmark_stages.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    origin_table.load_records(RECORD_COUNT)
    sdc_executor.benchmark_pipeline(pipeline, record_count=RECORD_COUNT)


# @pytest.mark.parametrize("threads", [1, 4, 8])
# def test_multithreading(threads):
#     pass
