# Copyright 2023 StreamSets Inc.

import logging
import string

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

        if Version(sdc_executor.version) >= Version('5.8.0'):
            assert 'ORACLE_01' in f'{exception.value.message}', f'Expected a ORACLE_01 error, got "{exception.value.message}" instead'
        else:
            assert 'ORACLE_02' in f'{exception.value.message}', f'Expected a ORACLE_02 error, got "{exception.value.message}" instead'

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
        logger.info(f'Adding rows into oracle ...')
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


def _execute_pipeline(sdc_executor, database, pipeline, wiretap, table_name, oracle_consumer):
    """
    Executes the given pipeline and verifies that the output is correct
    :param sdc_executor: data collector executor
    :param oracle: database object
    :param pipeline: pipeline to be executed
    :param table_name: name of the table
    :param oracle_consumer: oracle bulkload stage
    """

    table = _create_table(database, table_name, 'ID', 'NAME')

    try:
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Records are retrieved from different batches.
        output_records_values = [{'ID': record.field['ID'], 'NAME': record.field['NAME']}
                                 for record in wiretap.output_records]

        assert output_records_values == ROWS_IN_DATABASE

    finally:
        # Table is deleted
        logger.info(f'Dropping table {table_name} in Oracle...')
        table.drop(database.engine)
