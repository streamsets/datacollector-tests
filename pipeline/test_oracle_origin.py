# Copyright 2023 StreamSets Inc.

import logging
import string

import pytest
import sqlalchemy
from streamsets.testframework.markers import database, sdc_min_version, sdc_enterprise_lib_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

SRC_TABLE_PREFIX = 'ORATST'


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.skip('This tests is skipped since the offset has been disabled in the code')
def test_oracle_consumer_start_stop(sdc_builder, sdc_executor, database):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.
    We start and stop the pipeline to verify we write records to another table using JDBC_Producer in order to verify
    that all the records were read.

    The pipeline looks like this:
        oracle_consumer >> wiretap.destination
    """

    # Table name has as prefix ORATST. Column names in uppercase.
    table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')

    oracle_consumer.set_attributes(max_batch_size=50,
                                   stop_for_sql_exception=True,
                                   tables=[dict(schemaName='', tableName=table_name)])

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    # Configure and create table for Database.
    table = _create_table(database, table_name, 'ID', 'NAME')

    data = [dict(ID=i, NAME=get_random_string()) for i in range(1000, 6000)]
    try:
        logger.info('Adding rows into oracle ...')
        connection = database.engine.connect()
        connection.execute(table.insert(), data)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(5)
        sdc_executor.stop_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records

        assert len(data) == len(records)

        keys = list(data[0].keys())
        for record in records:
            # The data starts at ID=1000, so the index will be ID-1000
            origin_record = data[int(record.field.get('ID').value)-1000]
            for key in keys[1:]:
                assert record.field.get(key) == origin_record.get(key)

    finally:
        # Tables are deleted.
        logger.info(f'Dropping table {table_name} in oracle...')
        table.drop(database.engine)


@database('oracle')
@sdc_min_version('5.6.0')
@pytest.mark.skip('This tests is skipped since the offset has been disabled in the code')
def test_oracle_consumer_start_stop_multiple_blocks(sdc_builder, sdc_executor, database):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.

    We are inserting big volumes of data in multiple operations to force Oracle to create multiple blocks in disk.

    The pipeline looks like this:
        oracle_consumer >> wiretap.destination
    """

    # Table name has as prefix ORATST. Column names in uppercase.
    table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'
    table_name2 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')

    oracle_consumer.set_attributes(maximum_pool_size=4,
                                   tables=[dict(schemaName='', tableName=table_name1),
                                           dict(schemaName='', tableName=table_name2)])

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    # Configure and creates tables in database.
    table1 = _create_table(database, table_name1, 'ID', 'NAME')
    table2 = _create_table(database, table_name2, 'ID', 'NAME')

    data1 = [dict(ID=i, NAME=get_random_string()) for i in range(10000, 20000)]
    data2 = [dict(ID=i, NAME=get_random_string()) for i in range(20000, 30000)]
    data3 = [dict(ID=i, NAME=get_random_string()) for i in range(30000, 40000)]
    data4 = [dict(ID=i, NAME=get_random_string()) for i in range(40000, 50000)]
    data5 = [dict(ID=i, NAME=get_random_string()) for i in range(50000, 60000)]
    data6 = [dict(ID=i, NAME=get_random_string()) for i in range(60000, 70000)]
    data7 = [dict(ID=i, NAME=get_random_string()) for i in range(70000, 80000)]
    data8 = [dict(ID=i, NAME=get_random_string()) for i in range(80000, 90000)]

    try:
        logger.info(f'Adding rows into {database.type} database ...')
        connection = database.engine.connect()
        connection.execute(table1.insert(), data1)
        connection.execute(table2.insert(), data2)

        connection.execute(table1.insert(), data3)
        connection.execute(table2.insert(), data4)

        connection.execute(table1.insert(), data5)
        connection.execute(table2.insert(), data6)

        connection.execute(table1.insert(), data7)
        connection.execute(table2.insert(), data8)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(8)
        sdc_executor.stop_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        records = sorted(records, key=lambda x: x.field["ID"].value)

        raw_data = data1 + data2 + data3 + data4 + data5 + data6 + data7 + data8
        assert len(raw_data) == len(records)

        for i in range(len(records)):
            record = records[i].field
            for key in raw_data[i]:
                assert record.get(key) == raw_data[i].get(key)

    finally:
        # Tables are dropped.
        logger.info(f'Dropping table {table_name1} in oracle...')
        table1.drop(database.engine)
        logger.info(f'Dropping table {table_name2} in oracle...')
        table2.drop(database.engine)


@database('oracle')
@sdc_min_version('5.6.0')
def test_oracle_consumer_multiple_blocks(sdc_builder, sdc_executor, database):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.

    We are inserting big volumes of data in multiple operations to force Oracle to create multiple blocks in disk.

    The pipeline looks like this:
        oracle_consumer >> wiretap.destination
    """

    # Table name has as prefix ORATST. Column names in uppercase.
    table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'
    table_name2 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')

    oracle_consumer.set_attributes(maximum_pool_size=4,
                                   tables=[dict(schemaName='', tableName=table_name1),
                                           dict(schemaName='', tableName=table_name2)])

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    # Configure and creates tables in database.
    table1 = _create_table(database, table_name1, 'ID', 'NAME')
    table2 = _create_table(database, table_name2, 'ID', 'NAME')

    data1 = [dict(ID=i, NAME=get_random_string()) for i in range(10000, 20000)]
    data2 = [dict(ID=i, NAME=get_random_string()) for i in range(20000, 30000)]
    data3 = [dict(ID=i, NAME=get_random_string()) for i in range(30000, 40000)]
    data4 = [dict(ID=i, NAME=get_random_string()) for i in range(40000, 50000)]
    data5 = [dict(ID=i, NAME=get_random_string()) for i in range(50000, 60000)]
    data6 = [dict(ID=i, NAME=get_random_string()) for i in range(60000, 70000)]
    data7 = [dict(ID=i, NAME=get_random_string()) for i in range(70000, 80000)]
    data8 = [dict(ID=i, NAME=get_random_string()) for i in range(80000, 90000)]

    try:
        logger.info('Adding rows into oracle ...')
        connection = database.engine.connect()
        connection.execute(table1.insert(), data1)
        connection.execute(table2.insert(), data2)

        connection.execute(table1.insert(), data3)
        connection.execute(table2.insert(), data4)

        connection.execute(table1.insert(), data5)
        connection.execute(table2.insert(), data6)

        connection.execute(table1.insert(), data7)
        connection.execute(table2.insert(), data8)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=3000)

        records = wiretap.output_records
        records = sorted(records, key=lambda x: x.field["ID"].value)

        raw_data = data1 + data2 + data3 + data4 + data5 + data6 + data7 + data8
        assert len(raw_data) == len(records)

        for i in range(len(records)):
            record = records[i].field
            for key in raw_data[i]:
                assert record.get(key) == raw_data[i].get(key)

    finally:
        # Tables are dropped.
        logger.info(f'Dropping table {table_name1} in oracle...')
        table1.drop(database.engine)
        logger.info(f'Dropping table {table_name2} in oracle...')
        table2.drop(database.engine)


@database('oracle')
@sdc_min_version('5.6.0')
def test_oracle_consumer_wide_table(sdc_builder, sdc_executor, database):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.

    We are inserting big volumes of data with 20 columns to force Oracle to create multiple blocks in disk.

    The pipeline looks like this:
        oracle_consumer >> wiretap.destination
    """
    # Table name has as prefix ORATST. Column names in uppercase.
    table_name = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_uppercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')

    oracle_consumer.set_attributes(batches_per_request=20,
                                   maximum_pool_size=5,
                                   stop_for_sql_exception=True,
                                   tables=[dict(schemaName='', tableName=table_name)])

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database, database)

    # Add table to Database.
    table = _create_wide_table(database, table_name)

    data = [dict(ID=i, FIELD1=get_random_string(length=255), FIELD2=get_random_string(length=255),
                 FIELD3=get_random_string(length=255), FIELD4=get_random_string(length=255),
                 FIELD5=get_random_string(length=255), FIELD6=get_random_string(length=255),
                 FIELD7=get_random_string(length=255), FIELD8=get_random_string(length=255),
                 FIELD9=get_random_string(length=255), FIELD10=get_random_string(length=255),
                 FIELD11=get_random_string(length=255), FIELD12=get_random_string(length=255),
                 FIELD13=get_random_string(length=255), FIELD14=get_random_string(length=255),
                 FIELD15=get_random_string(length=255), FIELD16=get_random_string(length=255),
                 FIELD17=get_random_string(length=255), FIELD18=get_random_string(length=255),
                 FIELD19=get_random_string(length=255), FIELD20=get_random_string(length=255), ) for i in
            range(1000, 35000)]
    try:
        logger.info('Adding rows into oracle ...')
        connection = database.engine.connect()
        connection.execute(table.insert(), data)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=600)

        records = wiretap.output_records

        assert len(data) == len(records)

        keys = list(data[0].keys())
        for record in records:
            # The data starts at ID=1000, so the index will be ID-1000
            origin_record = data[int(record.field.get('ID').value)-1000]
            for key in keys[1:]:
                assert record.field.get(key) == origin_record.get(key)

    finally:
        # Tables are deleted.
        logger.info(f'Dropping table {table_name} in oracle...')
        table.drop(database.engine)


@database('oracle')
@sdc_min_version('5.6.0')
def test_oracle_consumer_multiple_blocks_case_sensitive(sdc_builder, sdc_executor, database):
    """ Insert records in Oracle database table using sqlalchemy.
    Then Oracle Origin is used to retrieve records from that table.

    We are using two tables with the same name but different case

    The pipeline looks like this:
        oracle_consumer >> wiretap.destination
    """

    # Table name has as prefix ORATST. Column names in uppercase.
    table_name1 = f'{SRC_TABLE_PREFIX}_{get_random_string(string.ascii_letters, 20)}'
    table_name2 = table_name1.upper()

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Oracle consumer tables set up to table_name.
    oracle_consumer = pipeline_builder.add_stage('Oracle Bulkload')

    oracle_consumer.set_attributes(maximum_pool_size=4,
                                   tables=[dict(schemaName='', tableName=table_name1),
                                           dict(schemaName='', tableName=table_name2)],
                                   case_sensitive=True,
                                   use_isolation_levels=True)

    wiretap = pipeline_builder.add_wiretap()
    oracle_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    # Configure and creates tables in database.
    table1 = _create_table(database, table_name1, 'ID', 'NAME')
    table2 = _create_table(database, table_name2, 'ID', 'NAME')

    data1 = [dict(ID=i, NAME=get_random_string()) for i in range(1000, 2000)]
    data2 = [dict(ID=i, NAME=get_random_string()) for i in range(2000, 3000)]

    try:
        logger.info('Adding rows into oracle ...')
        connection = database.engine.connect()
        connection.execute(table1.insert(), data1)
        connection.execute(table2.insert(), data2)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records_values = [{'ID': record.field['ID'], 'NAME': record.field['NAME']}
                                 for record in wiretap.output_records]

        data = data1 + data2
        assert len(data) == len(output_records_values)

        origin_data = data1 + data2
        for record in output_records_values:
            assert record in origin_data

    finally:
        # Tables are dropped.
        logger.info(f'Dropping table {table_name1} in oracle...')
        table1.drop(database.engine)
        logger.info(f'Dropping table {table_name2} in oracle...')
        table2.drop(database.engine)


def _create_table(database, table_name, primary_key_column, other_column):
    logger.info(f'Creating source table {table_name} in Database...')
    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column(primary_key_column, sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column(other_column, sqlalchemy.String(32)))
    table.create(database.engine)
    return table


def _create_wide_table(database, table_name):
    logger.info(f'Creating source table {table_name} in database...')
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('FIELD1', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD2', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD3', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD4', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD5', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD6', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD7', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD8', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD9', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD10', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD11', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD12', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD13', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD14', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD15', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD16', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD17', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD18', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD19', sqlalchemy.String(255)),
        sqlalchemy.Column('FIELD20', sqlalchemy.String(255)),
    )
    table.create(database.engine)
    return table
