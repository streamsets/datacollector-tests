import pytest
import os
import tempfile
import string
import sqlalchemy
import json

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string


DEFAULT_IMPALA_DB = 'default'
DEFAULT_KUDU_PORT = 7051

@stub
def test_admin_operation_timeout_in_milliseconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'case_sensitive': False}, {'case_sensitive': True}])
def test_case_sensitive(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_column_to_output_field_mapping(sdc_builder, sdc_executor):
    pass


@cluster('cdh')
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': False}, {'enable_local_caching': True}])
def test_enable_local_caching(sdc_builder, sdc_executor, stage_attributes, cluster):
    """
    We want to test that when the caching is enabled we get obsolete data.
    So we will insert a record in the storage and then we will create
    a file. The pipeline reads the file, then loads a record from the
    storage and replaces the value we have read from the file with the one
    we have read from the storage.

    If we update the record in the storage and then create the second file
    on the file system then due to the caching enabled the value in the
    file will be replaced with the storage value we have loaded before and
    the updated record will not be loaded.

    If the caching is disabled the new value will be used.

    The pipeline is as follows:

    Directory >> Kudu Lookup Processor >> Wiretap
    """

    dir_path = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_lowercase))
    table_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'JSON'
    directory.json_content = 'MULTIPLE_OBJECTS'
    directory.files_directory = dir_path
    directory.file_name_pattern = '*.json'
    directory.file_name_pattern_mode = 'GLOB'

    kudu = builder.add_stage('Kudu Lookup')
    kudu.kudu_table_name = f'impala::default.{table_name}'
    kudu.key_columns_mapping = [dict(field='/f1', columnName='id')]
    kudu.column_to_output_field_mapping = [dict(field='/d1', columnName='name', defaultValue='no_name')]
    kudu.missing_lookup_behavior = 'PASS_RECORD_ON'
    kudu.enable_table_caching = True
    kudu.eviction_policy_type = 'EXPIRE_AFTER_WRITE'
    kudu.expiration_time = 1
    kudu.time_unit = 'HOURS'
    kudu.set_attributes(**stage_attributes)

    wiretap = builder.add_wiretap()

    directory >> kudu >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String),
                             impala_partition_by='HASH PARTITIONS 16',
                             impala_stored_as='KUDU',
                             impala_table_properties={
                                 'kudu.master_addresses': f'{cluster.server_host}:{DEFAULT_KUDU_PORT}',
                                 'kudu.num_tablet_replicas': '1'
                             })

    engine = cluster.kudu.engine
    table.create(engine)

    try:
        sdc_executor.execute_shell(f'mkdir -p {dir_path}')
        sdc_executor.write_file(os.path.join(dir_path, 'a.json'), json.dumps({"f1": 1, "d1": "old_name1"}))

        conn = engine.connect()
        conn.execute(table.insert(), [{'id': 1, 'name': 'name1'}])

        status = sdc_executor.start_pipeline(pipeline)
        status.wait_for_pipeline_batch_count(2)

        conn.execute(table.update().where(table.c.id == 1).values(name='name2'))

        sdc_executor.write_file(os.path.join(dir_path, 'b.json'), json.dumps({"f1": 1, "d1": "old_name2"}))

        status.wait_for_pipeline_batch_count(4)

        output_records = [record.field for record in wiretap.output_records]

        if stage_attributes['enable_local_caching']:
            assert [{'f1': 1, 'd1': 'name1'}, {'f1': 1, 'd1': 'name1'}] == output_records
        else:
            assert [{'f1': 1, 'd1': 'name1'}, {'f1': 1, 'd1': 'name2'}] == output_records

    finally:
        try:
            sdc_executor.stop_pipeline(pipeline)
        finally:
            table.drop(engine)
            sdc_executor.execute_shell(f'rm -fr {dir_path}')


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_table_caching': False}, {'enable_table_caching': True}])
def test_enable_table_caching(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_ACCESS'},
                                              {'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_WRITE'}])
def test_eviction_policy_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_expiration_time(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_missing_value_in_matching_record': False},
                                              {'ignore_missing_value_in_matching_record': True}])
def test_ignore_missing_value_in_matching_record(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_key_columns_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_kudu_masters(sdc_builder, sdc_executor):
    pass


@stub
def test_kudu_table_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_maximum_entries_to_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_maximum_number_of_worker_threads(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_table_caching': True}])
def test_maximum_table_entries_to_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_lookup_behavior': 'PASS_RECORD_ON'},
                                              {'missing_lookup_behavior': 'SEND_TO_ERROR'}])
def test_missing_lookup_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@cluster('cdh')
@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'},
                                              {'multiple_values_behavior': 'FIRST_ONLY'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, stage_attributes, cluster):
    """
    We will test here that if there are many records in a DB, the first value will be taken
    if the FIRST_ONLY mode is used; or one record will be replaced with several records
    if the SPLIT_INTO_MULTIPLE_RECORDS mode is used.

    We put a JSON in a file, then we expect the KUDU processor to replace
    the d1 field with a name from the DB. At the first loop the DB is
    empty thus a default value will be used instead.
    After that we insert records in the DB and on the second loop the DB values
    will be used to replace the value from the file.

    The pipeline is as follows:

    Directory >> Kudu Lookup >> wiretap
    """
    dir_path = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_lowercase))
    table_name = get_random_string(string.ascii_lowercase, 10)
    json_content = "\n".join([json.dumps(r) for r in [
        {"f1": 1, "d1": "old_name1"},
        {"f1": 2, "d1": "old_name2"}
    ]])

    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'JSON'
    directory.json_content = 'MULTIPLE_OBJECTS'
    directory.files_directory = dir_path
    directory.file_name_pattern = '*.json'
    directory.file_name_pattern_mode = 'GLOB'

    kudu = builder.add_stage('Kudu Lookup')
    kudu.kudu_table_name = f'impala::default.{table_name}'
    kudu.key_columns_mapping = [dict(field='/f1', columnName='id')]
    kudu.column_to_output_field_mapping = [dict(field='/d1', columnName='name', defaultValue='no_name')]
    kudu.missing_lookup_behavior = 'PASS_RECORD_ON'
    kudu.enable_local_caching = True
    kudu.enable_table_caching = True
    kudu.retry_on_missing_value = True
    kudu.eviction_policy_type = 'EXPIRE_AFTER_WRITE'
    kudu.expiration_time = 1
    kudu.time_unit = 'HOURS'
    kudu.set_attributes(**stage_attributes)

    wiretap = builder.add_wiretap()

    directory >> kudu >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('index', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('id', sqlalchemy.Integer),
                             sqlalchemy.Column('name', sqlalchemy.String),
                             impala_partition_by='HASH PARTITIONS 16',
                             impala_stored_as='KUDU',
                             impala_table_properties={
                                 'kudu.master_addresses': f'{cluster.server_host}:{DEFAULT_KUDU_PORT}',
                                 'kudu.num_tablet_replicas': '1'
                             })

    engine = cluster.kudu.engine
    table.create(engine)

    try:
        sdc_executor.execute_shell(f'mkdir -p {dir_path}')
        sdc_executor.write_file(os.path.join(dir_path, 'a.json'), json_content)

        status = sdc_executor.start_pipeline(pipeline)
        status.wait_for_pipeline_batch_count(2)

        conn = engine.connect()
        conn.execute(table.insert(), [
            {'index': 11, 'id': 1, 'name': 'name1'}, {'index': 12, 'id': 1, 'name': 'name2'},
            {'index': 21, 'id': 2, 'name': 'nameA'}, {'index': 22, 'id': 2, 'name': 'nameB'}
        ])

        sdc_executor.write_file(os.path.join(dir_path, 'b.json'), json_content)

        status.wait_for_pipeline_batch_count(4)

        if stage_attributes['multiple_values_behavior'] == 'SPLIT_INTO_MULTIPLE_RECORDS':
            assert len(wiretap.output_records) == 6

            ids = [record.field['f1'].value for record in wiretap.output_records]
            assert [1, 2, 1, 1, 2, 2] == ids

            names = [record.field['d1'].value for record in wiretap.output_records]
            assert names[0:2] == ['no_name', 'no_name']
            assert set(names[2:4]) == set(['name1', 'name2'])
            assert set(names[4:6]) == set(['nameA', 'nameB'])

        else:
            assert len(wiretap.output_records) == 4
            expected_output = [
                {'id': 1, 'name': ['no_name']},
                {'id': 2, 'name': ['no_name']},
                {'id': 1, 'name': ['name1', 'name2']},
                {'id': 2, 'name': ['nameA', 'nameB']}
            ]
            for expected, actual in zip(expected_output, wiretap.output_records):
                assert expected['id'] == actual.field['f1'].value
                assert actual.field['d1'].value in expected['name']

    finally:
        try:
            sdc_executor.stop_pipeline(pipeline)
        finally:
            table.drop(engine)
            sdc_executor.execute_shell(f'rm -fr {dir_path}')


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_operation_timeout_in_milliseconds(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True, 'time_unit': 'DAYS'},
                                              {'enable_local_caching': True, 'time_unit': 'HOURS'},
                                              {'enable_local_caching': True, 'time_unit': 'MICROSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MILLISECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MINUTES'},
                                              {'enable_local_caching': True, 'time_unit': 'NANOSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'SECONDS'}])
def test_time_unit(sdc_builder, sdc_executor, stage_attributes):
    pass


@cluster('cdh')
@pytest.mark.parametrize('stage_attributes', [{'retry_on_missing_value': True}, {'retry_on_missing_value': False}])
@sdc_min_version('3.20.0')
def test_retry_on_missing_value(sdc_builder, sdc_executor, stage_attributes, cluster):
    """
    Normally if a value in a table is missing and a default value is
    specified, the default value is cached when local caching is enabled.
    We want to test that if the retry_on_missing_value field configuration
    is on, the default value is not cached and if next time a value
    appears in the table, that value is used even if the cache is enabled.

    We put a JSON in a file, then we expect the KUDU processor to replace
    the d1 field with a name from the DB. At the first loop the DB is
    empty thus a default value will be used instead.
    After that we insert a record in the DB and on the second loop
    depending on the retry_on_missing_value configuration the default value
    (the conf OFF) or the DB value (the conf is ON) will be used to replace
    the value from the file.

    The pipeline is as follows:

    Directory >> Kudu Lookup >> wiretap

    """
    dir_path = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_lowercase))
    table_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'JSON'
    directory.json_content = 'MULTIPLE_OBJECTS'
    directory.files_directory = dir_path
    directory.file_name_pattern = '*.json'
    directory.file_name_pattern_mode = 'GLOB'

    kudu = builder.add_stage('Kudu Lookup')
    kudu.kudu_table_name = f'impala::default.{table_name}'
    kudu.key_columns_mapping = [dict(field='/f1', columnName='id')]
    kudu.column_to_output_field_mapping = [dict(field='/d1', columnName='name', defaultValue='no_name')]
    kudu.missing_lookup_behavior = 'PASS_RECORD_ON'
    kudu.enable_table_caching = True
    kudu.enable_local_caching = True
    kudu.eviction_policy_type = 'EXPIRE_AFTER_ACCESS'
    kudu.expiration_time = 1
    kudu.time_unit = 'HOURS'
    kudu.set_attributes(**stage_attributes)

    wiretap = builder.add_wiretap()

    directory >> kudu >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String),
                             impala_partition_by='HASH PARTITIONS 16',
                             impala_stored_as='KUDU',
                             impala_table_properties={
                                 'kudu.master_addresses': f'{cluster.server_host}:{DEFAULT_KUDU_PORT}',
                                 'kudu.num_tablet_replicas': '1'
                             })

    engine = cluster.kudu.engine
    table.create(engine)

    try:
        sdc_executor.execute_shell(f'mkdir -p {dir_path}')
        sdc_executor.write_file(os.path.join(dir_path, 'a.json'), json.dumps({"f1": 1, "d1": "old_name1"}))

        status = sdc_executor.start_pipeline(pipeline)
        status.wait_for_pipeline_batch_count(2)

        conn = engine.connect()
        conn.execute(table.insert(), [{'id': 1, 'name': 'name1'}])

        sdc_executor.write_file(os.path.join(dir_path, 'b.json'), json.dumps({"f1": 1, "d1": "old_name2"}))

        status.wait_for_pipeline_batch_count(4)

        output_records = [record.field for record in wiretap.output_records]

        if stage_attributes['retry_on_missing_value']:
            assert [{'f1': 1, 'd1': 'no_name'}, {'f1': 1, 'd1': 'name1'}] == output_records
        else:
            assert [{'f1': 1, 'd1': 'no_name'}, {'f1': 1, 'd1': 'no_name'}] == output_records

    finally:
        try:
            sdc_executor.stop_pipeline(pipeline)
        finally:
            table.drop(engine)
            sdc_executor.execute_shell(f'rm -fr {dir_path}')
