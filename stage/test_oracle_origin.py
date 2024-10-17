# Copyright 2023 StreamSets Inc.

import logging
import string
import re
import pytest
import sqlalchemy
from streamsets.sdk.exceptions import RunError, RunningError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database, sdc_min_version, sdc_enterprise_lib_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

SRC_TABLE_PREFIX = 'ORATST'

ROWS_IN_DATABASE = [{'ID': 1, 'NAME': 'Tucu'}, {'ID': 2, 'NAME': 'Martin'}, {'ID': 3, 'NAME': 'Xavi'},
                    {'ID': 4, 'NAME': 'Alex'}, {'ID': 5, 'NAME': 'Danilo'}, {'ID': 6, 'NAME': 'Mel'},
                    {'ID': 7, 'NAME': 'Joan'}, {'ID': 8, 'NAME': 'Hugo'}, {'ID': 9, 'NAME': 'Toni'}]

ROWS_IN_DATABASE2 = [{'NUM': 1, 'NICKNAME': 'Tucu'}, {'NUM': 2, 'NICKNAME': 'Martin'}, {'NUM': 3, 'NICKNAME': 'Xavi'},
                     {'NUM': 4, 'NICKNAME': 'Alex'}, {'NUM': 5, 'NICKNAME': 'Danilo'}, {'NUM': 6, 'NICKNAME': 'Mel'},
                     {'NUM': 7, 'NICKNAME': 'Joan'}, {'NUM': 8, 'NICKNAME': 'Hugo'}, {'NUM': 9, 'NICKNAME': 'Toni'}]

NUM_ROWS_IN_BIG_DATABASE = 100000

ROWS_IN_BIG_DATABASE = []
for i in range(0, NUM_ROWS_IN_BIG_DATABASE):
    ROWS_IN_BIG_DATABASE.append({'ID': i, 'NAME': f'Dragon #{i}'})


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('case_sensitive', [True, False])
def test_oracle_consumer_single_table_single_thread(sdc_builder, sdc_executor, database, case_sensitive):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    Verify records are received correctly using wiretap.

    The pipeline looks like this:
        oracle_consumer >> wiretap
    """
    # Table name has as prefix ORATST. Column names in uppercase.
    if case_sensitive:
        table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_letters, 20)}'
    else:
        table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(tables=[dict(schemaName='', tableName=table_name)], case_sensitive=case_sensitive)

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    _execute_pipeline(sdc_executor, database, pipeline, wiretap, table_name, oracle_consumer)


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('case_sensitive', [True, False])
def test_oracle_consumer_read_empty_table(sdc_builder, sdc_executor, database, case_sensitive):
    """ Create an empty table in Oracle Database and verify that the connector reacts accordingly
    The pipeline looks like this:
        oracle_consumer >> wiretap
    """
    with database.engine.connect() as connection:
        version = connection.execute('SELECT * FROM V$VERSION').first()[0]
        if "Oracle Database 11g" in version:
            pytest.skip(f"This test (as well as the functionality) is not available on Oracle 11g: Database version: {version}")

    # Table name has as prefix ORATST. Column names in uppercase.
    if case_sensitive:
        table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_letters, 20)}'
    else:
        table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(tables=[dict(schemaName='%', tableName=table_name)], case_sensitive=case_sensitive)

    trash = pipeline_builder.add_stage('Trash')
    oracle_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    table = _create_table(database, table_name, 'ID', 'NAME')

    try:
        sdc_executor.add_pipeline(pipeline)
        with pytest.raises((RunError, RunningError)) as exception:
            sdc_executor.start_pipeline(pipeline=pipeline, wait_for_statuses=['FINISHED'], timeout_sec=30)

        expected_error_code = 'ORACLE_02'
        if Version(sdc_executor.version) >= Version('5.10.0'):
            expected_error_code = 'ORACLE_15'
        elif Version(sdc_executor.version) >= Version('5.8.0'):
            expected_error_code = 'ORACLE_01'

        assert expected_error_code in f'{exception.value.message}',\
            f'Expected a {expected_error_code} error, got "{exception.value.message}" instead'
    finally:
        logger.info(f'Dropping table {table_name} in Oracle...')
        table.drop(database.engine)


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('case_sensitive', [True, False])
def test_oracle_consumer_single_table_single_thread_with_table_wildcard(sdc_builder, sdc_executor, database,
                                                                        case_sensitive):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    Verify records are received correctly using wiretap.

    Table names contains wildcards.

    The pipeline looks like this:
        oracle_consumer >> wiretap
    """    # Table name has as prefix ORATST. Column names in uppercase.
    table_prefix = get_random_string(string.ascii_uppercase, 10)
    if case_sensitive:
        table_name = f'{SRC_TABLE_PREFIX}_{table_prefix}_{get_random_string(string.ascii_letters, 10)}'
    else:
        table_name = f'{SRC_TABLE_PREFIX}_{table_prefix}_{get_random_string(string.ascii_uppercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(tables=[dict(schemaName='', tableName=f'{SRC_TABLE_PREFIX}_{table_prefix}%')],
                                   case_sensitive=True)

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    _execute_pipeline(sdc_executor, database, pipeline, wiretap, table_name, oracle_consumer)


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('use_isolation_levels', [True, False])
def test_oracle_consumer_single_table_single_thread_with_schema_wildcard(sdc_builder, sdc_executor, database,
                                                                         use_isolation_levels):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    Verify records are received correctly using wiretap.

    Schema names contains wildcards.

    The pipeline looks like this:
        oracle_consumer >> wiretap
    """    # Table name has as prefix ORATST. Column names in uppercase.
    table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(tables=[dict(schemaName='%', tableName=table_name)],
                                   use_isolation_levels=use_isolation_levels)

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    _execute_pipeline(sdc_executor, database, pipeline, wiretap, table_name, oracle_consumer)


@database('oracle')
@sdc_min_version('5.6.0')
def test_oracle_consumer_single_table_single_thread_with_schema_and_table_wildcard(sdc_builder, sdc_executor, database):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    Verify records are received correctly using wiretap.

    Schema and table names contains wildcards.

    The pipeline looks like this:
        oracle_consumer >> wiretap
    """    # Table name has as prefix ORATST. Column names in uppercase.
    table_prefix = get_random_string(string.ascii_uppercase, 10)
    table_name = f'{SRC_TABLE_PREFIX}_{table_prefix}_{get_random_string(string.ascii_uppercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(tables=[dict(schemaName='%', tableName=f'{SRC_TABLE_PREFIX}_{table_prefix}%')])

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    _execute_pipeline(sdc_executor, database, pipeline, wiretap, table_name, oracle_consumer)


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('use_isolation_levels', [True, False])
def test_oracle_consumer_multitable_multithread(sdc_builder, sdc_executor, database, use_isolation_levels):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    Verify records are received correctly using wiretap.

    We are using multiple tables in the same schema.

    The pipeline looks like this:
        oracle_consumer >> wiretap
    """
    # Table names have as prefix ORATST. Column names in uppercase.
    table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'
    table_name2 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to two table_names.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(
        tables=[dict(schemaName='', tableName=table_name1), dict(schemaName=database.username, tableName=table_name2)],
        maximum_pool_size=2, use_isolation_levels=use_isolation_levels)

    wiretap = pipeline_builder.add_wiretap()

    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    # Configure and create table1 for Database.
    table1 = _create_table(database, table_name1, 'ID', 'NAME')

    # Configure and create table2 for Database.
    table2 = _create_table(database, table_name2, 'NUM', 'NICKNAME')

    try:
        # Insert data in both tables.
        connection = database.engine.connect()
        connection.execute(table1.insert(), ROWS_IN_DATABASE)
        connection.execute(table2.insert(), ROWS_IN_DATABASE2)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Records are retrieved from different batches. Records with ID, NAME match with ROWS_IN_DATABASE
        output_records_values = [{"ID": record.field['ID'], "NAME": record.field['NAME']}
                                 for record in wiretap.output_records
                                 if 'ID' in record.field.keys()]

        assert output_records_values == ROWS_IN_DATABASE

        # Records are retrieved from different batches. Records with ID, NAME match with ROWS_IN_DATABASE
        output_records_values = [{"NUM": record.field['NUM'], "NICKNAME": record.field['NICKNAME']}
                                 for record in wiretap.output_records
                                 if 'NUM' in record.field.keys()]

        assert output_records_values == ROWS_IN_DATABASE2

        # No more records were added
        output_records = [record for record in wiretap.output_records]

        assert len(ROWS_IN_DATABASE) + len(ROWS_IN_DATABASE2) == len(output_records)

    finally:
        # Table1 and Table2 are deleted
        logger.info(f'Dropping table {table_name1} in Oracle...')
        table1.drop(database.engine)
        logger.info(f'Dropping table {table_name2} in Oracle...')
        table2.drop(database.engine)


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('use_isolation_levels', [True, False])
@pytest.mark.parametrize('case_sensitive', [True, False])
def test_oracle_consumer_multitable_different_schemas_multithread(sdc_builder, sdc_executor, database,
                                                                  use_isolation_levels,
                                                                  case_sensitive):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    Verify records are received correctly using wiretap.

    We are using multiple tables in different schemas schema.

    The pipeline looks like this:
        oracle_consumer >> trash
    """
    # Table names have as prefix ORATST. Column names in uppercase.
    if case_sensitive:
        table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_letters, 20)}'
        table_name2 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_letters, 20)}'
    else:
        table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'
        table_name2 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    # As Oracle 19 is a multitenant container database, Oracle mandates to have the user name start with C##
    # https://community.oracle.com/tech/developers/discussion/4288833/ora-65096-invalid-common-user-or-role-name
    schema_name = f'C##_{get_random_string(string.ascii_uppercase)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to two table_names.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(
        tables=[dict(tableName=table_name1),
                dict(schemaName=schema_name, tableName=table_name2)],
        maximum_pool_size=2,
        case_sensitive=case_sensitive,
        use_isolation_levels=use_isolation_levels)

    wiretap = pipeline_builder.add_wiretap()

    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    # Create new schema.
    _create_schema(schema_name, database)

    # Configure and create table1 for Database.
    table1 = _create_table(database, table_name1, 'ID', 'NAME')

    # Configure table2 for Database.
    table2 = sqlalchemy.Table(
        table_name2,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('NUM', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('NICKNAME', sqlalchemy.String(32)),
        schema=schema_name
    )
    table2.create(database.engine)

    try:
        # Insert data in both tables.
        logger.info(f'Adding rows into Oracle ...')
        connection = database.engine.connect()
        connection.execute(table1.insert(), ROWS_IN_DATABASE)
        connection.execute(table2.insert(), ROWS_IN_DATABASE2)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Records are retrieved from different batches. Records with ID, NAME match with ROWS_IN_DATABASE
        output_records_values = [{"ID": record.field['ID'], "NAME": record.field['NAME']}
                                 for record in wiretap.output_records
                                 if 'ID' in record.field.keys()]

        assert output_records_values == ROWS_IN_DATABASE

        # Records are retrieved from different batches. Records with ID, NAME match with ROWS_IN_DATABASE
        output_records_values = [{"NUM": record.field['NUM'], "NICKNAME": record.field['NICKNAME']}
                                 for record in wiretap.output_records
                                 if 'NUM' in record.field.keys()]

        assert output_records_values == ROWS_IN_DATABASE2

        # No more records were added
        output_records = [record for record in wiretap.output_records]

        assert len(ROWS_IN_DATABASE) + len(ROWS_IN_DATABASE2) == len(output_records)

    finally:
        # Table1, Table2 and Schema are deleted
        logger.info(f'Dropping table {table_name1} in Oracle...')
        table1.drop(database.engine)
        logger.info(f'Dropping table {table_name2} in Oracle...')
        table2.drop(database.engine)

        logger.info(f'Dropping schema {schema_name} from database...')
        _drop_schema(schema_name, database)


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('case_sensitive', [True, False])
def test_oracle_consumer_multitable_different_schemas_same_tablename_multithread(sdc_builder, sdc_executor, database,
                                                                                 case_sensitive):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    Verify records are received correctly using wiretap.

    We are using multiple tables in different schemas schema, using the same tablename.

    The pipeline looks like this:
        oracle_consumer >> trash
    """
    # Table names have as prefix ORATST. Column names in uppercase.
    if case_sensitive:
        table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_letters, 20)}'
    else:
        table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    # As Oracle 19 is a multitenant container database, Oracle mandates to have the user name start with C##
    # https://community.oracle.com/tech/developers/discussion/4288833/ora-65096-invalid-common-user-or-role-name
    schema_name = f'C##_{get_random_string(string.ascii_uppercase)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to two table_names.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(tables=[dict(schemaName='', tableName=table_name1),
                                           dict(schemaName=schema_name, tableName=table_name1)],
                                   maximum_pool_size=2,
                                   case_sensitive=case_sensitive)

    wiretap = pipeline_builder.add_wiretap()

    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    # Create new schema.
    _create_schema(schema_name, database)

    # Configure and create table1 for Database.
    table1 = _create_table(database, table_name1, 'ID', 'NAME')

    # Configure table2 for Database.
    table2 = sqlalchemy.Table(
        table_name1,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('NUM', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('NICKNAME', sqlalchemy.String(32)),
        schema=schema_name
    )

    # Add table2 to Database.
    logger.info(f'Creating table {table_name1} in oracle ...')
    table2.create(database.engine)

    try:
        # Insert data in both tables.
        logger.info(f'Adding rows into oracle ...')
        connection = database.engine.connect()
        connection.execute(table1.insert(), ROWS_IN_DATABASE)
        connection.execute(table2.insert(), ROWS_IN_DATABASE2)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Records are retrieved from different batches. Records with ID, NAME match with ROWS_IN_DATABASE
        output_records_values = [{"ID": record.field['ID'], "NAME": record.field['NAME']}
                                 for record in wiretap.output_records
                                 if 'ID' in record.field.keys()]

        assert output_records_values == ROWS_IN_DATABASE

        # Records are retrieved from different batches. Records with ID, NAME match with ROWS_IN_DATABASE
        output_records_values = [{"NUM": record.field['NUM'], "NICKNAME": record.field['NICKNAME']}
                                 for record in wiretap.output_records
                                 if 'NUM' in record.field.keys()]

        assert output_records_values == ROWS_IN_DATABASE2

        # No more records were added
        output_records = [record for record in wiretap.output_records]

        assert len(ROWS_IN_DATABASE) + len(ROWS_IN_DATABASE2) == len(output_records)

    finally:
        # Table1, Table2 and Schema are deleted
        logger.info(f'Dropping table {table_name1} in oracle...')
        table1.drop(database.engine)
        logger.info(f'Dropping table {table_name1} in oracle...')
        table2.drop(database.engine)

        logger.info(f'Dropping schema {schema_name} from database...')
        _drop_schema(schema_name, database)


@database('oracle')
@sdc_min_version('5.6.0')
def test_oracle_consumer_table_finished_event(sdc_builder, sdc_executor, database):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    Verify records are received correctly using wiretap.

    We are inserting into two tables, and reading only one because the table-finished event stops the pipeline due to
    the pipeline finished executor.

    The pipeline looks like this:
        oracle_consumer >> wiretap
        oracle_consumer >= pipeline_finished_executor
    """
    # Table names have as prefix ORATST. Column names in uppercase.
    table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'
    table_name2 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to two table_names.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(
        tables=[dict(schemaName='', tableName=table_name1), dict(schemaName=database.username, tableName=table_name2)])

    wiretap = pipeline_builder.add_wiretap()

    pipeline_finished_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'table-finished'}"])

    oracle_consumer >> wiretap.destination
    oracle_consumer >= pipeline_finished_executor

    pipeline = pipeline_builder.build().configure_for_environment(database)

    # Configure and create table1 for Database.
    table1 = _create_table(database, table_name1, 'ID', 'NAME')

    # Configure and create table2 for Database.
    table2 = _create_table(database, table_name2, 'NUM', 'NICKNAME')

    try:
        # Insert data in both tables.
        logger.info('Adding rows into oracle...')
        connection = database.engine.connect()
        connection.execute(table1.insert(), ROWS_IN_DATABASE)
        connection.execute(table2.insert(), ROWS_IN_DATABASE2)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Records are retrieved from different batches. Records with ID, NAME match with ROWS_IN_DATABASE
        output_records_values = [{"ID": record.field['ID'], "NAME": record.field['NAME']}
                                 for record in wiretap.output_records]

        # We verify that the output is the same as only one of the inputs tables, since we stopped the pipeline with
        # the event, preventing it to read the second table.
        assert output_records_values == ROWS_IN_DATABASE
    finally:
        # Table1 and Table2 are deleted
        logger.info(f'Dropping table {table_name1} in oracle...')
        table1.drop(database.engine)
        logger.info(f'Dropping table {table_name2} in oracle...')
        table2.drop(database.engine)


@database('oracle')
@sdc_min_version('6.1.0')
@pytest.mark.parametrize('task_distribution', [1, 4, 100])
def test_oracle_consumer_task_distribution(sdc_builder, sdc_executor, database, task_distribution):
    """ This test aims to validate that the parameter Maximum Distributed Task works correctly.
    To do that, the test is sending 100000 records that are going to be divided in multiple tasks.
    Since the division depends on the data distributed among the different blocks in the table,
    the final number of task will be lower than the Maximum Distributed Task defined.
    To validate this test, we are going to check that all records are consumed correctly
    and that there is a log that says that there are multiple task created when Maximum Distributed Task > 1.
    To avoid this test to be flacky, we are checking an expected lower bound for the number of task created:
    Maximum Distributed Task = 1 -> lower bound = 1
    Maximum Distributed Task = 4 -> lower bound = 2
    Maximum Distributed Task = 100 -> lower bound = 15

    The pipeline looks like this:
        oracle_consumer >> wiretap
    """
    # Table name has as prefix ORATST. Column names in uppercase.
    table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(
        tables=[dict(schemaName='', tableName=table_name)],
        maximum_distributed_tasks=task_distribution)

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    _execute_pipeline(sdc_executor, database, pipeline, wiretap, table_name, oracle_consumer, True, 60)

    task_created = get_num_tasks_created(sdc_executor, pipeline)
    expected_lower_bound = 1
    if task_distribution == 1:
        expected_lower_bound = 1
    elif task_distribution == 4:
        expected_lower_bound = 2
    elif task_distribution == 100:
        expected_lower_bound = 15
    assert task_created >= expected_lower_bound, \
        (f'Expecting multiple task to more than {expected_lower_bound} '
         f'(task_created={task_created}, maximum_distributed_tasks={task_distribution})')
    assert task_created <= task_distribution, \
        f'Expecting equals or less than {task_distribution} tasks but found {task_created}'


@database('oracle')
@sdc_min_version('6.1.0')
@pytest.mark.parametrize('case_sensitive', [True, False])
def test_oracle_consumer_task_distribution_with_min_task_size_greater_than_data_size(
        sdc_builder, sdc_executor, database, case_sensitive):
    """ This test aims to validate that the parameter Minimum Task Size works correctly.
    To do that, the test is sending 100000 records that are divided in 3 or 4 task,
    and since Minimum Task Size is defined as 100000, all tasks have to be united in one task.
    To validate this test, we are going to check that all records are consumed correctly
    and that there is a log that says that we will create only one task.

    The pipeline looks like this:
        oracle_consumer >> wiretap
    """
    # Table name has as prefix ORATST. Column names in uppercase.
    if case_sensitive:
        table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_letters, 20)}'
    else:
        table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')
    oracle_consumer.set_attributes(
        tables=[dict(schemaName='', tableName=table_name)],
        case_sensitive=case_sensitive,
        minimum_task_size=NUM_ROWS_IN_BIG_DATABASE)

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)
    _execute_pipeline(sdc_executor, database, pipeline, wiretap, table_name, oracle_consumer, True)

    assert get_num_tasks_created(sdc_executor, pipeline) == 1, 'Expecting only one task to be created'


def get_num_tasks_created(sdc_executor, pipeline):
    """
    Gets the number of tasks created by checking the logs
    :param sdc_executor: data collector executor
    :param pipeline: pipeline to be executed
    """
    logs = sdc_executor.get_logs(pipeline=pipeline, severity='INFO')
    pattern = ".* tasks created to consume all the information.*"
    regex = re.compile(pattern)
    matches = re.findall(regex, str(logs))
    if len(matches) > 0:
        for match in matches:
            initial_position = match.find("OracleSource - ") + 14
            final_position = match.find("tasks created to consume all the information")
            task_created = int(match[initial_position:final_position].strip())
            logger.info("Tasks created: '%d'", task_created)
            return task_created
    else:
        assert False, (f'No log found with the pattern [{pattern}]. '
                       f'Maybe there are more than 70 INFO logs in this pipeline.')


def _create_schema(schema_name, database):
    """Create a new schema in the database.

    For Oracle, it creates a new user.

    Use ``_drop_schema()`` to remove schemas created by this function, to handle properly each case.

    Args:
        schema_name: (:obj:`str`) the schema name.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    database.engine.execute(f'CREATE USER {schema_name} IDENTIFIED BY {schema_name}')
    database.engine.execute(f'GRANT UNLIMITED TABLESPACE TO {schema_name}')


def _drop_schema(schema_name, database):
    """Remove a schema from the given database.

    Args:
        schema_name: (:obj:`str`) name of the schema to remove.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    database.engine.execute(f'DROP USER {schema_name}')


def _create_table(database, table_name, primary_key_column, other_column):
    logger.info(f'Creating source table {table_name} in Oracle...')
    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column(primary_key_column, sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column(other_column, sqlalchemy.String(32)))
    table.create(database.engine)
    return table


def _execute_pipeline(
        sdc_executor,
        database,
        pipeline,
        wiretap,
        table_name,
        oracle_consumer,
        use_big_database=False,
        wait_for_finished=30):
    """
    Executes the given pipeline and verifies that the output is correct
    :param sdc_executor: data collector executor
    :param database: database object
    :param pipeline: pipeline to be executed
    :param table_name: name of the table
    :param oracle_consumer: oracle bulkload stage
    :param use_big_database: insert tons of data or a few rows
    :param wait_for_finished: seconds to wait the pipeline to finish
    """

    table = _create_table(database, table_name, 'ID', 'NAME')
    data = ROWS_IN_BIG_DATABASE if use_big_database else ROWS_IN_DATABASE

    try:
        connection = database.engine.connect()
        connection.execute(table.insert(), data)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(wait_for_finished)

        # Records are retrieved from different batches.
        output_records_values = [{'ID': record.field['ID'], 'NAME': record.field['NAME']}
                                 for record in wiretap.output_records]
        output_records_values = sorted(output_records_values, key=lambda x: x['ID'])

        assert len(output_records_values) == len(data)
        for output_record, inserted_record in zip(output_records_values, data):
            assert output_record == inserted_record

    finally:
        # Table is deleted
        logger.info(f'Dropping table {table_name} in Oracle...')
        table.drop(database.engine)
