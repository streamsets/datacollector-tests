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
import re

import pytest
import sqlalchemy
import string

from contextlib import ExitStack
from requests.exceptions import JSONDecodeError
from random import randint
from time import sleep

from streamsets.sdk.exceptions import StartError, ValidationError
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

from stage.utils.common import cleanup
from stage.utils.utils_migration import LegacyHandler as PipelineHandler
from stage.utils.utils_oracle import (
    DefaultConnectionParameters,
    DefaultTableParameters,
    DefaultStartParameters,
    DefaultWaitParameters,
    DEFAULT_PK_COLUMN,
    DEFAULT_LOB_COLUMN,
    FETCH_PARAMETERS,
    FEAT_VER_FETCH_STRATEGY,
    MIN_ORACLE_VERSION,
    RELEASE_VERSION,
    NoError,
    StartMode,
    blob_from_file,
    database_version,
    oracle_stage_name,
    service_name,
    system_identifier,
    table_name,
    test_name,
    util_setup,
)


TRASH = "Trash"
DEFAULT_TIMEOUT_IN_SEC = 120

# precedence determines what happens first: pipeline start or table operations
PIPELINE, TABLE = "PIPELINE", "TABLE"
PRECEDENCES = [PIPELINE, TABLE]

logger = logging.getLogger(__name__)
pytestmark = [database("oracle"), sdc_min_version(RELEASE_VERSION)]


@pytest.fixture(scope="module")  # Cache the fixture to avoid reading/writing the BLOB each time the fixture is called
def blob_file_specs(sdc_builder, sdc_executor, database):
    return blob_from_file(database)


@pytest.mark.parametrize("precedence", PRECEDENCES)
def _test_template(
    sdc_builder, sdc_executor, database, database_version, oracle_stage_name, cleanup, test_name, table_name, precedence
):
    """This test provides a template to follow for tests. Following it is not compulsory, as some
    specific cases may have caveats that make it impractical to do so. It is however very useful
    to understand tests at a glance as well as keeping them simple.
    """

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    # Stage 1. Setup: prepare variables, create tables...
    table = sqlalchemy.Table(
        table_name, sqlalchemy.MetaData(), sqlalchemy.Column("id_column", sqlalchemy.Integer, primary_key=True)
    )

    connection = database.engine.connect()
    # The cleanup of resources is deferred right after obtaining them,
    # so no cleanup phase is required at the end
    cleanup(connection.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    # Create the oracle pipeline
    # The handler makes it much quicker to migrate tests to NEXT, it will be deprecated
    # once the transition is complete.
    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        **DefaultConnectionParameters(database)  # Default parameters to connect to the DB
        | DefaultTableParameters(table_name)  # Default parameters to include a specific table
        | DefaultStartParameters(database)  # Default parameters to start from the current SCN
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    # Stage 2. Execution: populate tables and insert data

    # Define a function to populate tables
    def populate_table():
        txn = connection.begin()
        connection.execute(...)
        txn.commit()

    # * Most tests will check two scenarios:
    #   1 - populate tables before the pipeline starts
    #   2 - populate tables after the pipeline starts
    if precedence != PIPELINE:  # case 1
        populate_table()

    cleanup(handler.stop_work, work)
    handler.start_work(work)

    if precedence == PIPELINE:  # case 2
        populate_table()

    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    # Stage 3. Assertion: verify if the results meet the test criteria
    assert len(wiretap.output_records) == 1


@pytest.mark.parametrize(
    "connection_type",
    [
        "SERVICE_NAME",
        "SYSTEM_IDENTIFIER",
        "TNS_CONNECTION",
        "TNS_ALIAS",
        "CONNECTION_URL",
    ],
)
@pytest.mark.parametrize("correct", [
    True,
    False
])
def test_connection_types(
    sdc_builder,
    sdc_executor,
    database,
    database_version,
    oracle_stage_name,
    cleanup,
    test_name,
    service_name,
    system_identifier,
    connection_type,
    correct,
):
    """Test every supported connection type. The pipeline must validate when
    @correct is True and fail the validation when @correct is false."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    username = database.username if correct else "wrong_username"
    password = database.password if correct else "wrong_password"
    host = database.host if correct else "fake.host.com"
    port = database.port if correct else 42

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
        ok_code = "0"
        assert cmd.exit_code == ok_code, f"Failed to create TNS default directory ({tns_dir}): {cmd}"
        cleanup(handler.execute_shell, f"rm -rf {tns_dir}")  # Defer removal of the directory
        cmd = handler.execute_shell(f"cat > {tns_file} << EOF\n{tns_file_content}\nEOF\n")
        assert cmd.exit_code == ok_code, f"Failed to create TNS default file ({tns_file}): {cmd}"

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
            "tns_alias": tns_alias,
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

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    # fmt: off
    oracle_cdc.set_attributes(
        connection_type=connection_type,
        **parameters
    )
    # fmt: on

    trash = pipeline_builder.add_stage(TRASH)

    oracle_cdc >> trash

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)
    if correct:
        # We want to make sure there are no validation issues and the connection can be established, but the pipeline
        # validation check is flaky, so we start a pipeline, wait for it to be running and then stop it
        handler.start_work(work)
        handler.wait_for_status(work, status='RUNNING')
        handler.stop_work(work)
    else:
        expected_error = 'ORACLE_CDC_0010'
        # The test framework will raise some of the expected validation errors as ValidationErrors, some as
        # StartErrors and others as JSONDecodeError
        with pytest.raises((ValidationError, StartError, JSONDecodeError)) as error:
            handler.start_work(pipeline)
        assert type(error.value) == JSONDecodeError or expected_error in error.value.message


@pytest.mark.parametrize(
    # fmt: off
    "buffer_size, expected_error", [
        [0, "VALIDATION_0035"],
        [1, None],
        [4096, None],
        [4097, "ORACLE_CDC_0000"],
    ]
    # fmt: on
)
def test_buffer_size(
    sdc_builder,
    sdc_executor,
    database,
    database_version,
    oracle_stage_name,
    cleanup,
    test_name,
    buffer_size,
    expected_error,
):
    """Check that the ring size is a multiple of 2."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    # fmt: off
    oracle_cdc.set_attributes(
        buffer_size=buffer_size,
        tables_filter=[{"schemasExclusionPattern": "SYS"}],
        **DefaultConnectionParameters(database)
    )
    # fmt:on

    trash = pipeline_builder.add_stage(TRASH)

    oracle_cdc >> trash

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    if expected_error is None:
        # We want to make sure there are no validation issues and the connection can be established, but the pipeline
        # validation check is flaky, so we start a pipeline, wait for it to be running and then stop it
        handler.start_work(work)
        handler.wait_for_status(work, status='RUNNING')
        handler.stop_work(work)
    else:
        with pytest.raises((ValidationError, StartError)) as error:
            handler.start_work(pipeline)
        assert expected_error in str(error.value)


@pytest.mark.parametrize(
    # fmt: off
    "start_mode, initial_parameter, initial_parameter_case",
    [
        ["CURRENT", None, "NONE"],
        ["CHANGE", "initial_system_change_number", "CURRENT_SCN"],
        ["CHANGE", "initial_system_change_number", "FUTURE_SCN"],
        ["INSTANT", "initial_instant", "CURRENT_INSTANT"],
        ["INSTANT", "initial_instant", "FUTURE_INSTANT"],
    ],
    # fmt: on
)
def test_start_mode(
    sdc_builder,
    sdc_executor,
    database,
    database_version,
    oracle_stage_name,
    cleanup,
    test_name,
    table_name,
    start_mode,
    initial_parameter,
    initial_parameter_case
):
    """Ensure that the stage starts from the specified point in the specified mode.
    The point can be an instant or a System Change Number (SCN)."""

    if initial_parameter_case == "NONE":
        initial_parameter_function, expected_error = None, NoError
    elif initial_parameter_case == "CURRENT_SCN":
        initial_parameter_function, expected_error = StartMode.current_scn, NoError
    elif initial_parameter_case == "FUTURE_SCN":
        initial_parameter_function, expected_error = StartMode.future_scn, StartError
    elif initial_parameter_case == "CURRENT_INSTANT":
        initial_parameter_function, expected_error = StartMode.current_instant, NoError
    elif initial_parameter_case == "FUTURE_INSTANT":
        initial_parameter_function, expected_error = StartMode.future_instant, StartError

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    table = sqlalchemy.Table(
        table_name, sqlalchemy.MetaData(), sqlalchemy.Column("ID", sqlalchemy.Integer, primary_key=True)
    )

    connection = database.engine.connect()
    cleanup(connection.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()
    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)

    # fmt: off
    oracle_cdc.set_attributes(
        start_mode=start_mode,
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name)
    )
    if initial_parameter is not None:
        oracle_cdc.set_attributes(**{
            initial_parameter: initial_parameter_function(database, cleanup)
        })
    # fmt: on
    trash = pipeline_builder.add_stage(TRASH)

    oracle_cdc >> trash

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    cleanup(handler.stop_work, work)
    try:
        handler.start_work(work).wait_for_status(work, "RUNNING", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    except expected_error:
        pass


def test_start_events(sdc_builder, sdc_executor, database, database_version, oracle_stage_name, cleanup, test_name):
    """Check that incarnation, instant and SCN values are sent in events when a pipeline with the
    Oracle CDC Origin stage is started."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    expected_event_type = "initial-database-state"
    expected_fields = ["incarnation", "instant", "system-change-number"]

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(**DefaultConnectionParameters(database))

    trash = pipeline_builder.add_stage(TRASH)

    event_wiretap = pipeline_builder.add_wiretap()

    oracle_cdc >> trash
    oracle_cdc >= event_wiretap.destination

    # Build and start the pipeline
    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)
    handler.start_work(work)
    cleanup(handler.stop_work, work)
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


@pytest.mark.parametrize("precedence", PRECEDENCES)
@pytest.mark.parametrize("session_wait_time", [0, 300])  # in ms
@pytest.mark.parametrize(
    # fmt: off
    "rows, columns", [
        [10**0, 10],
        [10**2, 10],
        [10**4, 10],
    ]
    # fmt: on
)
def test_basic_operations(
    sdc_builder,
    sdc_executor,
    database,
    database_version,
    oracle_stage_name,
    cleanup,
    table_name,
    test_name,
    precedence,
    rows,
    columns,
    session_wait_time,
    **kwargs,
):
    """Insert, update and delete n=@rows rows into a table and verify that each operation produces
    a record with the correct values."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    assert columns > 0, "number of columns must be greater than 0"

    # Create a unique id or primary key for each row that will be inserted
    unique_ids = [i for i in range(rows)]
    primary_column = "PRIMARY_COLUMN"  # Primary column will always be present
    non_primary_columns = [f"COLUMN_{i}" for i in range(columns)]  # n=@columns columns per row
    # Create a tuple with values (primary_column, column_0, column_1, ... column_n) definitions
    table_args = tuple(
        [table_name, sqlalchemy.MetaData(), sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True)]
        + [sqlalchemy.Column(column, sqlalchemy.Integer) for column in non_primary_columns]
    )
    # Table(primary_column, column_0, column_1, ..., column_n)
    table = sqlalchemy.Table(*table_args)

    # Data and statements to populate the table
    insert_records = [
        {**{primary_column: uid}, **{non_primary_columns[i]: uid for i in range(columns)}} for uid in unique_ids
    ]
    insert_stmt = table.insert()
    update_records = [
        {**{primary_column: uid}, **{non_primary_columns[i]: uid + 1 for i in range(columns)}} for uid in unique_ids
    ]
    update_stmt = (
        table.update()
        .where(getattr(table.c, primary_column) == sqlalchemy.bindparam(primary_column))
        .values({k: sqlalchemy.bindparam(k) for k in update_records[0].keys()})
    )
    delete_records = update_records[:]
    delete_stmt = table.delete().where(getattr(table.c, primary_column) == sqlalchemy.bindparam(primary_column))

    # Records expected as output from the Oracle CDC stage at the end of the test
    expected_records = insert_records + update_records + delete_records

    connection = database.engine.connect()
    cleanup(connection.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    # Build and start the pipeline.
    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()
    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        wait_time_before_session_start_in_ms=session_wait_time,
        wait_time_after_session_start_in_ms=session_wait_time,
        wait_time_after_session_end_in_ms=session_wait_time,
        **DefaultConnectionParameters(database)
        | DefaultTableParameters(table_name)
        | DefaultStartParameters(database)
    )
    oracle_cdc.set_attributes(**kwargs)

    wiretap = pipeline_builder.add_wiretap()
    oracle_cdc >> wiretap.destination
    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    def populate_table():
        # TODO mikel split into multiple transactions of lesser size
        txn = connection.begin()
        connection.execute(insert_stmt, insert_records)
        connection.execute(update_stmt, update_records)
        connection.execute(delete_stmt, delete_records)
        txn.commit()

    if precedence != PIPELINE:
        populate_table()

    cleanup(handler.stop_work, work)
    handler.start_work(work)

    if precedence == PIPELINE:
        populate_table()

    handler.wait_for_metric(work, "input_record_count", len(expected_records), timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]

    assert records == expected_records


@pytest.mark.parametrize("precedence", PRECEDENCES)
@pytest.mark.parametrize(
    # fmt: off
    "rows, columns", [
        [10 ** 1, 10],  # small transaction
        [10 ** 3, 20],  # medium transaction
        # [10 ** 5, 30],  # big transaction
    ]
    # fmt: on
)
def test_rollback(
    sdc_builder,
    sdc_executor,
    database,
    database_version,
    oracle_stage_name,
    cleanup,
    table_name,
    test_name,
    precedence,
    rows,
    columns,
    **kwargs,
):
    """Ensure no records are produced from transactions that were rolled back."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    assert columns > 0, "number of columns must be greater than 0"
    assert rows % 2 == 0, "number of rows must be even"

    # Create a unique id or primary key for each row that will be inserted
    unique_ids = [i for i in range(rows)]
    primary_column = "PRIMARY_COLUMN"  # Primary column will always be present
    non_primary_columns = [f"COLUMN_{i}" for i in range(columns)]  # n=@columns columns per row
    # Create a tuple with values (primary_column, column_0, column_1, ... column_n) definitions
    table_args = tuple(
        [table_name, sqlalchemy.MetaData(), sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True)]
        + [sqlalchemy.Column(column, sqlalchemy.Integer) for column in non_primary_columns]
    )
    # Table(primary_column, column_0, column_1, ..., column_n)
    table = sqlalchemy.Table(*table_args)

    # Data and statements to populate the table
    insert_records = [
        {**{primary_column: uid}, **{non_primary_columns[i]: uid for i in range(columns)}} for uid in unique_ids
    ]
    insert_stmt = table.insert()
    update_records = [
        {**{primary_column: uid}, **{non_primary_columns[i]: uid + 1 for i in range(columns)}} for uid in unique_ids
    ]
    update_stmt = (
        table.update()
        .where(getattr(table.c, primary_column) == sqlalchemy.bindparam(primary_column))
        .values({k: sqlalchemy.bindparam(k) for k in update_records[0].keys()})
    )

    # Post-rollback operation to verify data is being processed
    final_id = len(unique_ids)
    final_record = {**{primary_column: final_id}, **{non_primary_columns[i]: final_id for i in range(columns)}}
    final_operation = table.insert().values(final_record)

    # Records expected as output from the Oracle CDC stage at the end of the test
    expected_records = [final_record]

    connection = database.engine.connect()
    cleanup(connection.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    # Build and start the pipeline.
    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()
    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        **DefaultConnectionParameters(database)
        | DefaultTableParameters(table_name)
        | DefaultStartParameters(database)
        | DefaultWaitParameters()
    )
    oracle_cdc.set_attributes(**kwargs)

    wiretap = pipeline_builder.add_wiretap()
    oracle_cdc >> wiretap.destination
    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    def populate_table():
        txn = connection.begin()
        connection.execute(insert_stmt, insert_records)
        connection.execute(update_stmt, update_records)
        txn.rollback()
        txn = connection.begin()
        connection.execute(final_operation)
        txn.commit()

    if precedence != PIPELINE:
        populate_table()

    cleanup(handler.stop_work, work)
    handler.start_work(work)

    if precedence == PIPELINE:
        populate_table()

    handler.wait_for_metric(work, "input_record_count", len(expected_records), timeout_sec=30 * 60)

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]

    assert records == expected_records


@pytest.mark.parametrize("precedence", PRECEDENCES)
def test_long_sql_statements(
    sdc_builder, sdc_executor, database, database_version, oracle_stage_name, cleanup, table_name, test_name, precedence
):
    """Test SQL statements that contain a large amount of characters."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    primary_column = "ID_COLUMN"
    varchar_column = "VARCHAR_COLUMN"
    varchar_length = 4000
    record_count = 5

    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(varchar_column, sqlalchemy.VARCHAR(length=varchar_length)),
    )
    records = [{primary_column: i, varchar_column: f"{i}" * varchar_length} for i in range(record_count)]

    connection = database.engine.connect()
    cleanup(connection.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database)
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    def populate_table():
        txn = connection.begin()
        connection.execute(table.insert(), records)
        txn.commit()

    if precedence != PIPELINE:
        populate_table()

    cleanup(handler.stop_work, work)
    handler.start_work(work)

    if precedence == PIPELINE:
        populate_table()

    handler.wait_for_metric(work, "input_record_count", len(records), timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    output_records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert output_records == records


@pytest.mark.parametrize("precedence", PRECEDENCES)
def test_mixed_workload(
    sdc_builder, sdc_executor, database, database_version, oracle_stage_name, cleanup, table_name, test_name, precedence
):

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    pass


@pytest.mark.parametrize("precedence", PRECEDENCES)
def test_overlapping_transactions(
    sdc_builder, sdc_executor, database, database_version, oracle_stage_name, cleanup, table_name, test_name, precedence
):
    """Test the stage produces a correct output when transactions overlap. This test will:
    1. Start the pipeline
    2. Create a transaction (T1) but don't commit it
    3. Create and commit another transaction (T2)
    4. Verify that T2 produces output records
    5. Stop the pipeline
    6. Restart the pipeline
    7. Commit T1
    8. Verify T1 produces output records
    """

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    primary_column = "ID_COLUMN"
    record_count = 10
    wait_time = 5

    table = sqlalchemy.Table(
        table_name, sqlalchemy.MetaData(), sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True)
    )
    records_1 = [{primary_column: i} for i in range(record_count)]
    records_2 = [{primary_column: i} for i in range(record_count, record_count * 2)]

    connection_1 = database.engine.connect()
    cleanup(connection_1.close)
    connection_2 = database.engine.connect()
    cleanup(connection_2.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database)
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    txn1 = connection_1.begin()
    txn2 = connection_2.begin()

    def prepare_batch_1():
        connection_1.execute(table.insert(), records_1)

    def commit_batch_1():
        txn1.commit()

    def insert_batch_2():
        connection_2.execute(table.insert(), records_2)
        txn2.commit()

    prepare_batch_1()
    sleep(wait_time)
    insert_batch_2()

    cleanup(handler.stop_work, work)
    handler.start_work(work)

    handler.wait_for_metric(work, "input_record_count", len(records_2), timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    handler.stop_work(work)

    output_records_2 = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert output_records_2 == records_2
    wiretap.reset()

    commit_batch_1()

    handler.start_work(work)

    handler.wait_for_metric(work, "input_record_count", len(records_1), timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    output_records_1 = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert output_records_1 == records_1


@pytest.mark.parametrize("action", ["INCLUDE", "EXCLUDE"])
@pytest.mark.parametrize(
    # fmt: off
    "pattern",
    [
        r".*",
        r"a.z",
        r"a.*z",
        r"\.\*",
        r"a\.z",
        r"a\.\*z",
    ],
    # fmt: on
)
def test_oracle_cdc_inclusion_and_exclusion_pattern(
    sdc_builder, sdc_executor, database, database_version, oracle_stage_name, cleanup, test_name, pattern, action
):
    """Test patterns are included and excluded as expected.

    Create @table_count tables with different names that are matched by a different number of patterns.
    Test that they are included/excluded accordingly.
    """

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    primary_column = "ID_COLUMN"

    # Add a randomized prefix to avoid collisions if multiple tests are run simultaneously
    # The prefix will only contain lowercase ascii letters to avoid affecting the pattern
    random_prefix = get_random_string().lower()
    pattern = f"{random_prefix}{pattern}"
    default_inclusion_pattern = f"{random_prefix}.*"
    default_exclusion_pattern = ""

    include_pattern = pattern if action == "INCLUDE" else default_inclusion_pattern
    exclude_pattern = default_exclusion_pattern if action == "INCLUDE" else pattern

    table_count = 10
    records_per_table = 1
    table_names = [f"{random_prefix}A{'X' * i}Z" for i in range(table_count - 1)]
    table_names.append(f"{random_prefix}ZA")  # will never match the pattern

    tables = [
        sqlalchemy.Table(table_names[i], sqlalchemy.MetaData(), sqlalchemy.Column(primary_column, sqlalchemy.Integer))
        for i in range(table_count)
    ]

    for table in tables:
        table.create(database.engine)
        cleanup(table.drop)

    connection = database.engine.connect()
    cleanup(connection.close)

    ip = re.compile(include_pattern, re.IGNORECASE)
    included_tables = set(table.name for table in tables if ip.fullmatch(table.name))
    ep = re.compile(exclude_pattern, re.IGNORECASE)
    excluded_tables = set(table.name for table in tables if ep.fullmatch(table.name))

    # Remove excluded tables from excluded tables
    for excluded_table in excluded_tables:
        if excluded_table in included_tables:
            included_tables.remove(excluded_table)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        tables_filter=[{"tablesInclusionPattern": include_pattern, "tablesExclusionPattern": exclude_pattern}],
        **DefaultConnectionParameters(database) | DefaultStartParameters(database) | DefaultWaitParameters(),
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    records = [{primary_column: i} for i in range(table_count * records_per_table)]
    expected_records = []
    txn = connection.begin()
    for i in range(table_count):
        table = tables[i]
        record_subset = records[records_per_table * i : records_per_table * (i + 1)]
        if table.name in included_tables:
            expected_records += record_subset
        connection.execute(table.insert(), record_subset)
    txn.commit()

    cleanup(handler.stop_work, work)
    handler.start_work(work)

    handler.wait_for_metric(work, "input_record_count", len(expected_records), timeout_sec=30)

    output_records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]

    assert output_records == expected_records


@pytest.mark.parametrize("precedence", PRECEDENCES)
def test_decimal_attributes(
    sdc_builder, sdc_executor, database, database_version, oracle_stage_name, cleanup, table_name, test_name, precedence
):
    """Test the precision and scale attributes of the decimal type."""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    primary_column = "ID_COLUMN"
    decimal_column = "DECIMAL_COLUMN"
    precision = 20
    scale = 2
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(decimal_column, sqlalchemy.Numeric(20, 2)),
    )
    records = [{primary_column: 1, decimal_column: 42.42}]

    connection = database.engine.connect()
    cleanup(connection.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database)
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    def populate_table():
        txn = connection.begin()
        connection.execute(table.insert(), records)
        txn.commit()

    if precedence != PIPELINE:
        populate_table()

    cleanup(handler.stop_work, work)
    handler.start_work(work)

    if precedence == PIPELINE:
        populate_table()

    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    output_records = wiretap.output_records
    assert len(output_records) == 1
    output_record = output_records[0]
    decimal_attributes = output_record.get_field_attributes(f"/{decimal_column}")
    assert decimal_attributes["precision"] == f"{precision}"
    assert decimal_attributes["scale"] == f"{scale}"


@pytest.mark.parametrize("precedence", PRECEDENCES)
@pytest.mark.parametrize(
    # fmt: off
    "batches, max_batch_size", [
        [1, 1],
        [1, 10],
        [10, 1],
        [100, 100],
    ]
    # fmt: on
)
def test_batch_size(
    sdc_builder,
    sdc_executor,
    database,
    database_version,
    oracle_stage_name,
    cleanup,
    table_name,
    test_name,
    precedence,
    batches,
    max_batch_size,
):
    """Verify that the stage procudes batches with at most the specified max batch size"""

    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    total_records = batches * max_batch_size
    id_column = "ID_COLUMN"
    table = sqlalchemy.Table(
        table_name, sqlalchemy.MetaData(), sqlalchemy.Column(id_column, sqlalchemy.Integer, primary_key=True)
    )

    connection = database.engine.connect()
    cleanup(connection.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        max_batch_size_in_records=max_batch_size,
        max_batch_wait_time_in_ms=-1,
        **DefaultConnectionParameters(database) | DefaultTableParameters(table_name) | DefaultStartParameters(database),
    )

    wiretap = pipeline_builder.add_wiretap()

    oracle_cdc >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    def populate_table():
        txn = connection.begin()
        for i in range(total_records):
            connection.execute(table.insert().values({id_column: i}))
        txn.commit()

    if precedence != PIPELINE:
        populate_table()

    cleanup(handler.stop_work, work)
    handler.start_work(work)

    if precedence == PIPELINE:
        populate_table()

    handler.wait_for_metric(work, "input_record_count", batches * max_batch_size, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    expected_records = [{id_column: i} for i in range(total_records)]
    assert len(records) == len(expected_records)
    assert all([record in records for record in expected_records])


@pytest.mark.parametrize("precedence", PRECEDENCES)
@pytest.mark.parametrize("fetch_strategy, fetch_overflow", FETCH_PARAMETERS)
@pytest.mark.parametrize("session_wait_time", [0, 1000])
def test_fetch_strategy(
    sdc_builder,
    sdc_executor,
    database,
    database_version,
    oracle_stage_name,
    table_name,
    test_name,
    precedence,
    fetch_strategy,
    fetch_overflow,
    session_wait_time,
):
    """Run some existing tests with different fetch strategies and ensure they are unaffected"""

    if Version(sdc_builder.version) < FEAT_VER_FETCH_STRATEGY:
        pytest.skip("Fetch strategies are unsupported in this version")

    logger.info("Run 'test_basic_operations'")
    with ExitStack() as cleanup:
        test_basic_operations(
            sdc_builder,
            sdc_executor,
            database,
            database_version,
            oracle_stage_name,
            cleanup.callback,
            table_name + "A",
            test_name + "with_basic_operations",
            precedence,
            400,  # rows
            5,    # columns
            session_wait_time,
            fetch_strategy=fetch_strategy,
            fetch_overflow=fetch_overflow,
            wait_time_before_session_start_in_ms=session_wait_time,
            wait_time_after_session_start_in_ms=session_wait_time,
            wait_time_after_session_end_in_ms=session_wait_time,
        )

    logger.info("Run 'test_rollback'")
    with ExitStack() as cleanup:
        test_rollback(
            sdc_builder,
            sdc_executor,
            database,
            database_version,
            oracle_stage_name,
            cleanup.callback,
            table_name + "B",
            test_name + "with_rollback",
            precedence,
            400,  # rows
            5,    # columns
            fetch_strategy=fetch_strategy,
            fetch_overflow=fetch_overflow,
            wait_time_before_session_start_in_ms=session_wait_time,
            wait_time_after_session_start_in_ms=session_wait_time,
            wait_time_after_session_end_in_ms=session_wait_time,
        )

    # Commented out to speed up the test
    # logger.info("Run 'test_overlapping_transactions'")
    # with ExitStack() as cleanup:
    #     test_overlapping_transactions(
    #         sdc_builder,
    #         sdc_executor,
    #         database,
    #         database_version,
    #         cleanup,
    #         table_name + "C",
    #         test_name + "with_overlapping_transactions",
    #         precedence,
    #         fetch_strategy=fetch_strategy,
    #         fetch_overflow=fetch_overflow,
    #     )


@sdc_min_version("5.8.1")
def test_set_lob_locator(
        sdc_builder,
        sdc_executor,
        database,
        oracle_stage_name,
        cleanup,
        database_version,
        table_name,
        test_name,
        blob_file_specs,
):
    """
    Perform several LOB operations that create a SET_LOB_OPERATOR in the redo-log and
    verify that they are not scanned by LogMiner. (COLLECTOR-4598)

    SELECT_LOB_LOCATOR will be generated before every LOB operation.
    We verify that operations that come after a set of LOB_OPERATIONS
    have been correctly parsed and converted into records.
    For that we: LOB_WRITE, LOB_TRIM, LOB_ERASE and INSERT.
    """


    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    # Create a unique id or primary key for each row that will be inserted
    primary_column = DEFAULT_PK_COLUMN  # Primary column will always be present
    blob_column = DEFAULT_LOB_COLUMN
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path, file_name, file_length, file_content = blob_file_specs
    primary_key = randint(10000, 100000)

    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(blob_column, sqlalchemy.BLOB),
    )

    connection = database.engine.connect()
    cleanup(connection.close)

    table.create(database.engine)
    cleanup(table.drop, database.engine)

    # Build and start the pipeline.
    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()
    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        large_data_types=[{"dataType": "BLOB", "maxSize": -1}],
        **DefaultConnectionParameters(database)
        | DefaultTableParameters(table_name)
        | DefaultStartParameters(database),
    )

    wiretap = pipeline_builder.add_wiretap()
    oracle_cdc >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    handler.start_work(work)
    handler.cleanup(handler.stop_work, work)

    txn = connection.begin()

    # LOB_WRITE.
    connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
    connection.execute(
        f"""DECLARE
                dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                f_lob BFILE;
                b_lob BLOB;
            BEGIN
                f_lob := bfilename(dir, imgFile);
                INSERT INTO {table_name} VALUES ({primary_key}, EMPTY_BLOB())
                RETURNING {blob_column} INTO b_lob;

                DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));
                COMMIT;
                DBMS_LOB.FILECLOSE(f_lob);
            END;"""
    )

    # LOB_TRIM.
    trim_length = 10
    connection.execute(
        f"""DECLARE
                loc_b BLOB;
            BEGIN
                SELECT "{blob_column}" INTO loc_b FROM {table_name} WHERE {primary_column} = {primary_key} FOR UPDATE;
                dbms_lob.trim(loc_b, {trim_length});
                COMMIT;
            END;"""
    )

    # LOB_ERASE.
    erase_length = 1
    erase_offset = 1
    connection.execute(
        f"""DECLARE
                loc_b BLOB;
                e_len NUMBER := {erase_length};
            BEGIN
                SELECT "{blob_column}" INTO loc_b FROM {table_name} WHERE {primary_column} = {primary_key} FOR UPDATE;
                dbms_lob.erase(loc_b, e_len, {erase_offset});
                COMMIT;
            END;"""
    )

    connection.execute(table.insert().values({primary_column: primary_key+1, blob_column: None}))
    txn.commit()

    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    insert_record = wiretap.output_records[-1]
    assert insert_record.field[primary_column] == primary_key+1

    status = sdc_executor.get_pipeline_status(pipeline).response.json()
    assert status.get("status") not in {"RUN_ERROR", "RUNNING_ERROR"} or "ORACLE_CDC_1137" not in status.get("message")


@sdc_min_version("5.8.1")
def test_lob_ops_with_lobs_disabled(
        sdc_builder,
        sdc_executor,
        database,
        oracle_stage_name,
        cleanup,
        database_version,
        table_name,
        test_name,
        blob_file_specs,
):
    """
    Perform several LOB operations and verify they are not scanned by LogMiner if
    LOB support is disabled in the stage. (COLLECTOR-4622)

    1. Create two tables: with and without lob, A and B respectively.
    2. Set the Oracle stage to track both tables.
    3. Insert some values in A.
    4. Do LOB operations in B.
    5. Insert more values in A.
    6. Check that the two inserts of A are present.
    """
    if database_version < MIN_ORACLE_VERSION:
        pytest.skip(f"Oracle version {database_version} is not officially supported")

    # Create a unique id or primary key for each row that will be inserted
    primary_column = DEFAULT_PK_COLUMN  # Primary column will always be present
    blob_column = DEFAULT_LOB_COLUMN
    scanned_column = "SCANNED"
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path, file_name, file_length, file_content = blob_file_specs
    primary_key = randint(10000, 100000)
    scanned_value = randint(10000, 100000)

    table_with_lob_name = table_name + "1"
    table_without_lob_name = table_name + "2"

    table_with_lob = sqlalchemy.Table(
        table_with_lob_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(blob_column, sqlalchemy.BLOB),
    )

    table_without_lob = sqlalchemy.Table(
        table_without_lob_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column(primary_column, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(scanned_column, sqlalchemy.Integer),
    )

    connection = database.engine.connect()
    cleanup(connection.close)

    table_with_lob.create(database.engine)
    cleanup(table_with_lob.drop, database.engine)
    table_without_lob.create(database.engine)
    cleanup(table_without_lob.drop, database.engine)

    # Build and start the pipeline.
    handler = PipelineHandler(sdc_builder, sdc_executor, database, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()
    oracle_cdc = pipeline_builder.add_stage(name=oracle_stage_name)
    oracle_cdc.set_attributes(
        large_data_types=[],
        tables_filter=[{"tablesInclusionPattern": f"{table_name}.*"}],
        **DefaultConnectionParameters(database)
        | DefaultStartParameters(database),
    )

    wiretap = pipeline_builder.add_wiretap()
    oracle_cdc >> wiretap.destination

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    handler.start_work(work)
    cleanup(handler.stop_work, work)

    txn = connection.begin()

    # Insert operation (scanned).
    connection.execute(table_without_lob.insert({primary_column: primary_key, scanned_column: scanned_value}))

    # LOB_WRITE (not scanned).
    connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
    connection.execute(
        f"""DECLARE
                dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                f_lob BFILE;
                b_lob BLOB;
            BEGIN
                f_lob := bfilename(dir, imgFile);
                INSERT INTO {table_with_lob_name} VALUES ({primary_key}, EMPTY_BLOB())
                RETURNING {blob_column} INTO b_lob;

                DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));
                COMMIT;
                DBMS_LOB.FILECLOSE(f_lob);
            END;"""
    )

    # LOB_TRIM (not scanned).
    trim_length = 10
    connection.execute(
        f"""DECLARE
                loc_b BLOB;
            BEGIN
                SELECT "{blob_column}" INTO loc_b FROM {table_with_lob_name} WHERE {primary_column} = {primary_key} FOR UPDATE;
                dbms_lob.trim(loc_b, {trim_length});
                COMMIT;
            END;"""
    )

    # LOB_ERASE (not scanned).
    erase_length = 1
    erase_offset = 1
    connection.execute(
        f"""DECLARE
                loc_b BLOB;
                e_len NUMBER := {erase_length};
            BEGIN
                SELECT "{blob_column}" INTO loc_b FROM {table_with_lob_name} WHERE {primary_column} = {primary_key} FOR UPDATE;
                dbms_lob.erase(loc_b, e_len, {erase_offset});
                COMMIT;
            END;"""
    )

    # Second insert operation.
    connection.execute(table_without_lob.insert({primary_column: primary_key+1, scanned_column: scanned_value+1}))
    txn.commit()

    # Wait for the 2 insert operations.
    handler.wait_for_metric(work, "input_record_count", 2, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    assert len(wiretap.output_records) >= 2
    status = sdc_executor.get_pipeline_status(pipeline).response.json()
    assert status.get("status") not in {"RUN_ERROR", "RUNNING_ERROR"} or "ORACLE_CDC_1137" not in status.get("message")

