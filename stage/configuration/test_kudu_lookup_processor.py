import pytest
import os
import tempfile
import string
import sqlalchemy
import json

from streamsets.testframework.decorators import stub
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.markers import cluster, sdc_min_version


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

        output_records = [{
            'id': record.field['f1'].value,
            'name': record.field['d1'].value
        } for record in wiretap.output_records]

        if stage_attributes['enable_local_caching']:
            assert [{'id': 1, 'name': 'name1'}, {'id': 1, 'name': 'name1'}] == output_records
        else:
            assert [{'id': 1, 'name': 'name1'}, {'id': 1, 'name': 'name2'}] == output_records

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


@stub
@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


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
