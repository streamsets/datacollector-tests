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

import logging
import pytest
import string

from datetime import datetime, timedelta
from json import JSONDecodeError

from streamsets.sdk.exceptions import ValidationError
from streamsets.sdk.sdc_api import StartError
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

from stage.utils.utils_migration import LegacyHandler as PipelineHandler
from stage.utils.utils_oracle import (
    DefaultConnectionParameters,
    cleanup,
    service_name,
    system_identifier,
    test_name,
    util_setup,
)


RELEASE_VERSION = "5.4.0"
ORACLE_CDC_ORIGIN = "Oracle CDC"
TRASH = "Trash"
DEFAULT_TIMEOUT_IN_SEC = 120

OK_STATUS = "0"

logger = logging.getLogger(__name__)
pytestmark = [database("oracle"), sdc_min_version(RELEASE_VERSION)]


@pytest.mark.parametrize(
    "connection_type", ("SERVICE_NAME", "SYSTEM_IDENTIFIER", "TNS_CONNECTION", "TNS_ALIAS", "CONNECTION_URL")
)
@pytest.mark.parametrize("correct", (True, False))
def test_connection_types(
    sdc_builder, sdc_executor, database, cleanup, test_name, service_name, system_identifier, connection_type, correct
):
    """Test every supported connection type. The pipeline must validate when
    @correct is True and fail the validation when @correct is false."""

    username = database.username if correct else "wrong_username"
    password = database.password if correct else "wrong_password"
    host = database.host if correct else "fake.host.com"
    port = database.port if correct else "42"

    # Set up the variables any connection type might need
    tns_alias = "STREAMSETS"
    tns_dir = f"/tmp/sdc-{get_random_string(string.ascii_letters, 10)}"
    tns_file = f"{tns_dir}/tnsnames.ora"
    # fmt: off
    tns_connection = f"(DESCRIPTION = "\
                     f"(ADDRESS = (PROTOCOL = TCP) (HOST = {host}) (PORT = {port})) "\
                     f"(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = {service_name})))"
    # fmt: on
    tns_file_content = f"{tns_alias} = {tns_connection}"

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    # Create the tnsnames.ora file required for TNS_ALIAS
    if connection_type == "TNS_ALIAS":
        cmd = handler.execute_shell(f"mkdir -p {tns_dir}")
        assert cmd.exit_code == OK_STATUS, f"Failed to create TNS default directory ({tns_dir}): {cmd}"
        cleanup.callback(handler.execute_shell, f"rm -rf {tns_dir}")  # Defer removal of the directory
        cmd = handler.execute_shell(f"cat > {tns_file} << EOF\n{tns_file_content}\nEOF\n")
        assert cmd.exit_code == OK_STATUS, f"Failed to create TNS default file ({tns_file}): {cmd}"

    # Every connection type will use the shared parameters
    shared_parameters = {"username": username, "password": password}
    # Unique parameters are specific to each connection type
    # fmt: off
    unique_parameters = {
        "SERVICE_NAME": {
            "host": host,
            "port": port,
            "service_name": service_name
        },
        "SYSTEM_IDENTIFIER": {
            "host": host,
            "port": port,
            "system_identifier": system_identifier
        },
        "TNS_CONNECTION": {
            "tns_connection": tns_connection
        },
        "TNS_ALIAS": {
            "tns_connection": tns_alias,
            "connection_properties": [{"key": "oracle.net.tns_admin", "value": tns_dir}],
        },
        "CONNECTION_URL": {
            "connection_url": f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
        },
    }
    # fmt: on
    # Merge the shared and unique parameters
    parameters = {**shared_parameters, **unique_parameters[connection_type]}

    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)
    # fmt: off
    oracle_cdc_origin.set_attributes(
        connection_type=connection_type,
        **parameters
    )
    # fmt: on

    trash = pipeline_builder.add_stage(TRASH)

    oracle_cdc_origin >> trash

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    handler.add_pipeline(pipeline)
    if correct:
        handler.validate_pipeline(pipeline)
        # ToDo mikel check specific error
    else:
        # The test framework will raise some of the expected validation errors
        # as ValidationError and others as JSONDecodeError
        with pytest.raises((ValidationError, JSONDecodeError)):
            handler.validate_pipeline(pipeline)


@pytest.mark.parametrize(
    # fmt: off
    "buffer_size, correct", [
        [0, False],
        [1, True],
        [4096, True],
        [4097, False],
    ]
    # fmt: on
)
def test_buffer_size(sdc_builder, sdc_executor, database, cleanup, test_name, buffer_size, correct):
    """Check that the ring size is a multiple of 2."""
    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)
    # fmt: off
    oracle_cdc_origin.set_attributes(
        buffer_size=buffer_size,
        **DefaultConnectionParameters(database)
    )
    # fmt:on

    trash = pipeline_builder.add_stage(TRASH)

    oracle_cdc_origin >> trash

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    handler.add_pipeline(pipeline)
    if correct:
        handler.validate_pipeline(pipeline)
    else:
        with pytest.raises(ValidationError):
            handler.validate_pipeline(pipeline)
            # ToDo mikel check specific error


def test_initial_scn(sdc_builder, sdc_executor, database, cleanup, test_name):
    """Test that the pipeline fails if the Initian System Change Number is greater then
    than the last SCN of the database."""

    connection = database.engine.connect()
    cleanup.callback(connection.close)
    try:
        last_scn = int(connection.execute("SELECT CURRENT_SCN FROM V$DATABASE").first()[0])
    except Exception as ex:
        pytest.fail(f"Could not retrieve last SCN: {ex}")

    # Use a considerably greater SCN to ensure the database SCN doesn't catch up while
    # the test is running
    scn = last_scn + 100

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)

    # fmt: off
    oracle_cdc_origin.set_attributes(
        start_mode="CHANGE",
        initial_system_change_number=scn,
        **DefaultConnectionParameters(database)
    )
    # fmt: on

    trash = pipeline_builder.add_stage(TRASH)

    oracle_cdc_origin >> trash

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    with pytest.raises(StartError):
        cleanup.callback(handler.stop_work, work)
        handler.start_work(work)


def test_initial_instant(sdc_builder, sdc_executor, database, cleanup, test_name):
    """Test that the pipeline fails if the start instant is greater than the last SCN of the database."""

    # ToDo mikel: check if DB is in DST

    oracle_date_format = "DD-MM-YYYY HH24:MM:SS"
    python_date_format = "%d-%m-%Y %H:%M:%S"

    connection = database.engine.connect()
    cleanup.callback(connection.close)
    try:
        current_instant = connection.execute(f"SELECT TO_CHAR(SYSDATE, '{oracle_date_format}') FROM DUAL").first()[0]
    except Exception as ex:
        pytest.fail(f"Could not retrieve current database instant: {ex}")

    # Increase the instant by an hour
    instant = datetime.strptime(current_instant, python_date_format) + timedelta(hours=1)
    initial_instant = instant.strftime(python_date_format)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)

    # fmt: off
    oracle_cdc_origin.set_attributes(
        start_mode="INSTANT",
        initial_instant=initial_instant,
        **DefaultConnectionParameters(database)
    )
    # fmt: on

    trash = pipeline_builder.add_stage(TRASH)

    oracle_cdc_origin >> trash

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    with pytest.raises(StartError):
        cleanup.callback(handler.stop_work, work)
        handler.start_work(work)


def test_dst_instant(sdc_builder, sdc_executor, database):
    # ToDo mikel
    pass


def test_offset_incarnation(sdc_builder, sdc_executor, database):
    # ToDo mikel
    pass


def test_start_events(sdc_builder, sdc_executor, database, cleanup, test_name):
    """Check that incarnation, instant and SCN values are sent in events when a pipeline with the
    Oracle CDC Origin stage is started."""

    expected_event_type = "initial-database-state"
    expected_fields = ["incarnation", "instant", "system-change-number"]

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc_origin = pipeline_builder.add_stage(ORACLE_CDC_ORIGIN)
    oracle_cdc_origin.set_attributes(**DefaultConnectionParameters(database))

    trash = pipeline_builder.add_stage(TRASH)

    event_wiretap = pipeline_builder.add_wiretap()

    oracle_cdc_origin >> trash
    oracle_cdc_origin >= event_wiretap.destination

    # Build and start the pipeline
    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)
    handler.start_work(work)
    cleanup.callback(handler.stop_work, work)
    handler.wait_for_status(work, "RUNNING", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    # Find the event containing the database state
    database_state = None
    for record in event_wiretap.output_records:
        if record.header.values["sdc.event.type"] == expected_event_type:
            database_state = record.field
            break
    assert database_state is not None, f"Could not find {expected_event_type} event"

    # Ensure none of the expected fields are missing
    missing_fields = []
    for expected_field in expected_fields:
        if expected_field not in database_state:
            missing_fields.append(expected_field)
    assert len(missing_fields) == 0, f"Fields '{','.join(missing_fields)}' missing from {expected_event_type} event"

