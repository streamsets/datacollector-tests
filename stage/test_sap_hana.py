# Copyright 2020 StreamSets Inc.
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
import math
import string
import time
from operator import itemgetter

import pytest
import sqlalchemy
from streamsets.testframework.markers import sap_hana, sdc_min_version
from streamsets.testframework.utils import get_random_string
from streamsets.sdk.exceptions import ValidationError

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'ID': 1, 'NAME': 'M. Indurain'},
    {'ID': 2, 'NAME': 'M. Pantani'},
    {'ID': 3, 'NAME': 'J. Ullrich'}
]

# https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/20a1569875191014b507cf392724b7eb.html
@sdc_min_version('3.17.0')
@sap_hana
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', [
    ('VARBINARY(5)', "CAST('Hello' AS VARBINARY(5))", 'BYTE_ARRAY', 'SGVsbG8='),
    ('BOOLEAN', "TRUE", 'BOOLEAN', True),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NVARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('SHORTTEXT(5)', "'Hello'", 'STRING', 'Hello'),
    ('ALPHANUM(5)', "'Hello'", 'STRING', 'Hello'),
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('TIME', "'14:25:10'", 'TIME', 51910000),
    ('SECONDDATE', "'2004-05-23 14:25:10.00'", 'DATETIME', 1085322310000),
    ('TIMESTAMP', "'2004-05-23 14:25:10.00'", 'DATETIME', 1085322310000),
    ('BLOB', "'HELLO'", 'BYTE_ARRAY', 'SEVMTE8='),
    ('CLOB', "'CLOB'", 'STRING', 'CLOB'),
    ('NCLOB', "'NCLOB'", 'STRING', 'NCLOB'),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ('BINTEXT', "'Hello'", 'STRING', 'Hello'),
    ('TINYINT', '255', 'SHORT', 255),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('INTEGER', '-2147483648', 'INTEGER', '-2147483648'),
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('DECIMAL(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('SMALLDECIMAL', '5.20', 'DECIMAL', '5.2'),
    ('REAL', '5.20', 'FLOAT', '5.2'),
    ('DOUBLE', '5.20', 'DOUBLE', '5.2'),
    ('FLOAT(1)', '5.20', 'FLOAT', '5.2')
])
def test_types(sdc_builder, sdc_executor, database, sql_type, insert_fragment,
               expected_type, expected_value):
    """
        Test all feasible SAP HANA types.
        We insert two different records for each data type. One with the data value and another one
        with a NULL value.
        We use a pipeline:
            SAP HANA Query Consumer >> wiretap
        Both outputs (the value and the null) must have the correct data type.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (id INTEGER, data_column {sql_type} NULL, PRIMARY KEY(id))")

        logger.info(f"Table {table_name} created. Inserting values...")

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        logger.info("Values inserted.")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = 'SELECT * FROM {0}'.format(table_name)
        origin.incremental_mode = False

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 2
        record = wiretap.output_records[0]
        null_record = wiretap.output_records[1]

        assert record.field['DATA_COLUMN'].type == expected_type
        assert null_record.field['DATA_COLUMN'].type == expected_type

        assert record.field['DATA_COLUMN']._data['value'] == expected_value
        assert null_record.field['DATA_COLUMN'] == None
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
def test_consumer_offset_resume(sdc_builder, sdc_executor, database):
    """
    Ensure that the Query consumer can resume where it ended and stop the pipeline when it reads all the data.

    We use a pipeline:
        SAP HANA Query Consumer >> wiretap
        SAP HANA Query Consumer >> Finisher

    The test run the pipeline three times. Each time, before it runs the pipeline it inserts one row to the database
    and the test ensures the pipeline outputs the new record inserted to test the offset works fine after resuming the
    pipeline.
    """

    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('NAME', sqlalchemy.String(32))
    )

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('SAP HANA Query Consumer')
    origin.incremental_mode = True
    origin.sql_query = 'SELECT * FROM {0} WHERE '.format(table_name) + 'id > ${OFFSET} ORDER BY id'
    origin.initial_offset = '0'
    origin.offset_column = 'ID'

    wiretap = pipeline_builder.add_wiretap()

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> wiretap.destination
    origin >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        connection = database.engine.connect()

        for i in range(len(ROWS_IN_DATABASE)):
            wiretap.reset()
            # Insert one row to the database
            connection.execute(table.insert(), [ROWS_IN_DATABASE[i]])

            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.output_records) == 1
            assert wiretap.output_records[0].get_field_data('/ID') == i + 1

            sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('3.17.0')
@sap_hana
@pytest.mark.parametrize('batch_size', [1, 3, 10])
def test_consumer_non_incremental_mode(sdc_builder, sdc_executor, database, batch_size):
    """Ensure that the Query consumer works properly in non-incremental mode.

    We test the stage with different batch sizes, since the logic is different depending on the whole table
    fit in a batch or not (see e.g. SDC-14882 for an issue with the second case).

    Pipeline:  sap_hana_consumer >> wiretap
               sap_hana_consumer >= finisher

    """
    num_records = 8
    input_data = [{'ID': i, 'NAME': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('SAP HANA Query Consumer')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=batch_size)
    wiretap = pipeline_builder.add_wiretap()
    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> wiretap.destination
    origin >= finisher
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create and populate table
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('NAME', sqlalchemy.String(32)))
        table.create(database.engine)
        connection = database.engine.connect()
        connection.execute(table.insert(), input_data)

        # Run the pipeline and check the stage consumed all the expected records. Repeat several times to
        # ensure non-incremental mode works as expected after restarting the pipeline.
        for _ in range(3):
            wiretap.reset()
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            records = wiretap.output_records
            records.sort(key=_sortRecordsById)

            assert len(input_data) == len(records)
            assert input_data == [record.field for record in records]

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('3.17.0')
@sap_hana
def test_duplicate_column_labels(sdc_builder, sdc_executor, database):
    """
        Tests the validation raises an error when the query contains two columns
        with the same column name.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INTEGER, DATA_COLUMN INTEGER NULL, PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created.")
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        # The select query two times the same table so it will get the columns repeated.
        origin.sql_query = f"SELECT * FROM {table_name} T, {table_name} TB WHERE " \
            "T.P_ID > ${OFFSET} ORDER BY T.P_ID ASC LIMIT 10;"
        origin.incremental_mode = True
        origin.offset_column = "P_ID"

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(ValidationError) as e:
            sdc_executor.validate_pipeline(pipeline)

        assert e.value.issues['issueCount'] == 1
        exception_message = e.value.issues['stageIssues'][origin.instance_name][0]['message']
        assert "JDBC_31 -" in exception_message

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
def test_empty_result_set(sdc_builder, sdc_executor, database):
    """
        Tests the output records is 0 if the table is empty
        We will use a pipeline:
            SAP HANA Query Consumer >> Wiretap
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INTEGER, DATA_COLUMN INTEGER NULL, PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created.")
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"SELECT * FROM {table_name} WHERE " \
            "P_ID > ${OFFSET} ORDER BY P_ID ASC LIMIT 10;"
        origin.incremental_mode = True
        origin.offset_column = "P_ID"

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        time.sleep(10)  # We let the pipeline run for few seconds to be sure that we are just not going to fast
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 0

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
@pytest.mark.parametrize('batch_size', [1, 3, 10])
def test_consumer_incremental_mode(sdc_builder, sdc_executor, database, batch_size):
    """Ensure that the Query consumer works properly in incremental mode.

    We test the stage with different batch sizes, since the logic is different depending on the whole table
    fit in a batch or not (see e.g. SDC-14882 for an issue with the second case).

    Pipeline:  sap_hana_consumer >> wiretap
    """

    num_records = 8
    input_data = [{'ID': i, 'NAME': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} WHERE' \
        ' ID > ${OFFSET} ORDER BY ID ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('SAP HANA Query Consumer')
    origin.set_attributes(incremental_mode=True,
                          sql_query=sql_query,
                          offset_column='ID',
                          max_batch_size_in_records=batch_size)
    wiretap = pipeline_builder.add_wiretap()

    origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create and populate table
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('NAME', sqlalchemy.String(32)))
        table.create(database.engine)
        connection = database.engine.connect()
        connection.execute(table.insert(), input_data)

        # Run the pipeline and check the stage consumed all the expected records. Repeat several times to
        # ensure non-incremental mode works as expected after restarting the pipeline.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(input_data))
        sdc_executor.stop_pipeline(pipeline)
        sdc_records = [record.field for record in wiretap.output_records]
        sdc_records = sorted(sdc_records, key=itemgetter('ID')) # sort record by ID, wiretap can alter original order
        assert input_data == sdc_records

        # Run a second time should output 0 new records
        wiretap.reset()
        sdc_executor.start_pipeline(pipeline)
        time.sleep(10)  # We let the pipeline run for few seconds to be sure that we are just not going to fast
        sdc_executor.stop_pipeline(pipeline)

        sdc_records = [record.field for record in wiretap.output_records]
        assert len(sdc_records) == 0

        # Insert a new data and run again. Check the records output are the ones we inserted this last time.
        input_data_again = [{'ID': i, 'NAME': get_random_string()} for i in range(9, (num_records*2) + 1)]
        connection.execute(table.insert(), input_data_again)

        wiretap.reset()
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(input_data_again))
        sdc_executor.stop_pipeline(pipeline)
        records = wiretap.output_records
        records.sort(key=_sortRecordsById)
        sdc_records = [record.field for record in records]
        assert input_data_again == sdc_records
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('3.17.0')
@sap_hana
def test_invalid_query(sdc_builder, sdc_executor, database):
    """
        Tests the validation raises an error when the query is not valid.
        In this case we use SELET instead of SELECT. That should raise the
        error on the validation.
    """
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('SAP HANA Query Consumer')
    origin.sql_query = f"SELET * FORM TABLE WHERE " \
        "P_ID > ${OFFSET} ORDER BY P_ID LIMIT 10;"
    origin.incremental_mode = True
    origin.offset_column = "P_ID"

    trash = builder.add_stage('Trash')

    origin >> trash

    pipeline = builder.build().configure_for_environment(database)

    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(ValidationError) as e:
        sdc_executor.validate_pipeline(pipeline)
    logger.info(f"Issue count: {e.value.issues['issueCount']}")
    assert e.value.issues['issueCount'] == 1
    exception_message = e.value.issues['stageIssues'][origin.instance_name][0]['message']
    assert "JDBC_34 -" in exception_message


@sdc_min_version('3.17.0')
@sap_hana
def test_jdbc_decimal_headers(sdc_builder, sdc_executor, database):
    """
        Tests the output records contains header values indicating metadata info
        about the records retrieved from the database.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INT NOT NULL, DEC DECIMAL(2, 1), PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created. Inserting decimal value")

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, 1.5)")

        logger.info('Decimal value inserted.')

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"SELECT * FROM {table_name} WHERE " \
            "P_ID > ${OFFSET} ORDER BY P_ID ASC LIMIT 10;"
        origin.incremental_mode = True
        origin.offset_column = "P_ID"

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert 1.5 == wiretap.output_records[0].field['DEC']
        assert wiretap.output_records[0].header.values['jdbc.DEC.scale'] == '1'
        assert wiretap.output_records[0].header.values['jdbc.DEC.precision'] == '2'

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
def test_lineage_events(sdc_builder, sdc_executor, database):
    """
        Tests the lineage events.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    num_records = 4
    input_data = [{'P_ID': i, 'NAME': get_random_string(string.ascii_lowercase, 5)} for i in range(1, num_records + 1)]

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        table = sqlalchemy.Table(table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column('P_ID', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('NAME', sqlalchemy.String(5)))
        table.create(database.engine)
        logger.info(f"Table {table_name} created. Inserting rows")

        # Insert some rows
        connection.execute(table.insert(), input_data)

        logger.info('Rows inserted.')

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"SELECT * FROM {table_name} WHERE " \
            "P_ID > ${OFFSET} ORDER BY P_ID ASC LIMIT 10;"
        origin.incremental_mode = True
        origin.offset_column = "P_ID"

        records_wiretap = builder.add_wiretap()
        events_wiretap = builder.add_wiretap()

        origin >> records_wiretap.destination
        origin >= events_wiretap.destination

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_records)
        sdc_executor.stop_pipeline(pipeline)

        assert len(records_wiretap.output_records) == 4
        assert len(events_wiretap.output_records) == 1
        assert events_wiretap.output_records[0].header['values']['sdc.event.type'] == 'jdbc-query-success'
        assert events_wiretap.output_records[0].field['rows'] == 4
        assert 'query' in events_wiretap.output_records[0].field
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
@pytest.mark.parametrize('query_end', ["WHERE T.P_ID > ${OFFSET} LIMIT 10", "", "ORDER BY P_ID ASC LIMIT 10"])
def test_missing_clause(sdc_builder, sdc_executor, database, query_end):
    """
        Tests the validation raises an error when the query does not contain
        the where or the order by clause.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INTEGER, DATA_COLUMN INTEGER NULL, PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created.")
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"SELECT * FROM {table_name} T, {table_name} TB {query_end};"
        origin.incremental_mode = True
        origin.offset_column = "P_ID"

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(ValidationError) as e:
            sdc_executor.validate_pipeline(pipeline)

        assert e.value.issues['issueCount'] == 1
        exception_message = e.value.issues['stageIssues'][origin.instance_name][0]['message']
        assert 'JDBC_38 -' in exception_message

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
def test_multiline_query(sdc_builder, sdc_executor, database):
    """
        Test the stage works OK if the query has carry returns.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INTEGER, DATA_COLUMN INTEGER NULL, PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created. Inserting one row")
        connection.execute(f"INSERT INTO {table_name} VALUES(1, 1)")
        logger.info("Inserted one row")
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"SELECT * FROM {table_name} WHERE\nP_ID >" \
            " ${OFFSET}\nORDER BY P_ID ASC LIMIT 10;"
        origin.incremental_mode = True
        origin.offset_column = "P_ID"

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['DATA_COLUMN'] == 1
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
@pytest.mark.parametrize('offset_column,expected_error', [('T.P_ID', 'JDBC_32 -'), ('NONEXISTINGCOLUMN', 'JDBC_29 -')])
def test_invalid_offset_column(sdc_builder, sdc_executor, database, offset_column, expected_error):
    """
        Tests the validation raises an error when the query does not contain
        a valid offset column config.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INTEGER, DATA_COLUMN INTEGER NULL, PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created.")
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"SELECT * FROM {table_name} T " \
            "WHERE T.P_ID > ${OFFSET} ORDER BY T.P_ID ASC LIMIT 10;"
        origin.incremental_mode = True
        origin.offset_column = offset_column

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(ValidationError) as e:
            sdc_executor.validate_pipeline(pipeline)

        assert e.value.issues['issueCount'] == 1
        exception_message = e.value.issues['stageIssues'][origin.instance_name][0]['message']
        logger.info(f"exception e: {exception_message}")
        assert expected_error in exception_message

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
def test_qualified_offset_column_in_query(sdc_builder, sdc_executor, database):
    """Qualified offset column on the query string but not on the offset column config. """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INTEGER, DATA_COLUMN INTEGER NULL, PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created.")
        connection.execute(f"INSERT INTO {table_name} VALUES(1, 1)")
        logger.info("Inserted one row")
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"SELECT * FROM {table_name} T " \
            "WHERE T.P_ID > ${OFFSET} ORDER BY T.P_ID ASC LIMIT 10;"
        origin.incremental_mode = True
        origin.offset_column = "P_ID"

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['DATA_COLUMN'] == 1

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.17.0')
@sap_hana
def test_stored_procedure(sdc_builder, sdc_executor, database):
    """
        Test if we can call a stored procedure.
        We create one table and we insert one row. Then we create a stored procedure that performs a select
        on this table.
        We create a pipeline that calls this stored procedure and we check we read the data from the table we
        created.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    stored_procedure_name = get_random_string(string.ascii_uppercase, 10)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INTEGER, DATA_COLUMN INTEGER NULL, PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created.")
        connection.execute(f"INSERT INTO {table_name} VALUES(1, 1)")
        logger.info("Inserted one row. Creating the Stored Procedure.")
        connection.execute(f'CREATE PROCEDURE "{stored_procedure_name}"( ) LANGUAGE SQLSCRIPT SQL SECURITY INVOKER READS SQL DATA AS BEGIN SELECT * FROM {table_name}; END')
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"CALL {stored_procedure_name}()"
        origin.incremental_mode = False
        origin.offset_column = "P_ID"

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['DATA_COLUMN'] == 1

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")
        logger.info('Dropping procedure %s in %s database...', stored_procedure_name, database.type)
        connection.execute(f"DROP PROCEDURE {stored_procedure_name}")


@sdc_min_version('3.17.0')
@sap_hana
def test_timestamp_as_string(sdc_builder, sdc_executor, database):
    """
        Test the timestamp as string config. We create a table with a timestamp field
        and we add one row. We run a pipeline to read that row and we check we got
        an string representig the timestamp.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    connection = database.engine.connect()
    try:
        # Create table
        logger.info(f"Creating table: {table_name}")
        connection.execute(f"CREATE TABLE {table_name} (P_ID INTEGER, DATA_COLUMN TIMESTAMP NULL, PRIMARY KEY(P_ID))")
        logger.info(f"Table {table_name} created.")
        connection.execute(f"INSERT INTO {table_name} VALUES(1, '2004-05-23 14:25:10.00')")
        logger.info("Inserted one row.")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SAP HANA Query Consumer')
        origin.sql_query = f"SELECT * from {table_name} T WHERE " \
            "T.P_ID > ${OFFSET} ORDER BY T.P_ID"
        origin.incremental_mode = True
        origin.offset_column = "P_ID"
        origin.convert_timestamp_to_string = True

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['DATA_COLUMN'] == '2004-05-23 14:25:10.0'

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")

def _sortRecordsById(r):
    return r.field['ID'].value
