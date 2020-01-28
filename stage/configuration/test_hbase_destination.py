import json
import logging

import pytest

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def version_check(sdc_builder, cluster):
    if cluster.version == 'cdh6.0.0' and Version('3.5.0') <= Version(sdc_builder.version) < Version('3.6.0'):
        pytest.skip('HBase destination is not included in streamsets-datacollector-cdh_6_0-lib in SDC 3.5')


@cluster('cdh', 'hdp')
def test_fields(sdc_builder, sdc_executor, cluster, keep_data):
    """Test that fields are properly mapped into HBase."""
    DATA = [{'team': 'Quick-Step Floors', 'points': '13322.9'},
            {'team': 'Team Sky', 'points': '9718.9'},
            {'team': 'BORA - hansgrohe', 'points': '9138'}]
    # Since it comes out of HBase, we expect output to be lexicographically sorted bytes and to see
    # column family:column qualifer formatting.
    EXPECTED_OUTPUT = [{b'BORA - hansgrohe': {b'data:points': b'9138'}},
                       {b'Quick-Step Floors': {b'data:points': b'13322.9'}},
                       {b'Team Sky': {b'data:points': b'9718.9'}}]
    table_name = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    # Set JSON Content to "Array of Objects" to make it easier to pass in DATA without extra processing.
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(DATA))

    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.set_attributes(fields=[dict(columnValue='/points', columnStorageType='TEXT', columnName='data:points')],
                         row_key='/team',
                         table_name=table_name)

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    dev_raw_data_source >> [hbase, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'data': {}})
        sdc_executor.start_pipeline(pipeline)

        table = cluster.hbase.client.table(table_name)
        # HappyBase's table.scan returns a row key and a dictionary of column data, which we roll up with a list
        # comprehension.
        assert [{row_key: data} for row_key, data in table.scan()] == EXPECTED_OUTPUT

    finally:
        if not keep_data:
            logger.info('Deleting HBase table %s ...', random_table_name)
            cluster.hbase.client.delete_table(name=table_name, disable=True)


@stub
def test_hbase_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_hbase_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
def test_hbase_user(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_invalid_column': False, 'implicit_field_mapping': True},
                                              {'ignore_invalid_column': True, 'implicit_field_mapping': True}])
def test_ignore_invalid_column(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_missing_field': False}, {'ignore_missing_field': True}])
def test_ignore_missing_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'implicit_field_mapping': False}, {'implicit_field_mapping': True}])
def test_implicit_field_mapping(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'kerberos_authentication': False}, {'kerberos_authentication': True}])
def test_kerberos_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_row_key(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'storage_type': 'BINARY'}, {'storage_type': 'TEXT'}])
def test_storage_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass


@stub
def test_time_basis(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'validate_table_existence': False}, {'validate_table_existence': True}])
def test_validate_table_existence(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_zookeeper_client_port(sdc_builder, sdc_executor):
    pass


@stub
def test_zookeeper_parent_znode(sdc_builder, sdc_executor):
    pass


@stub
def test_zookeeper_quorum(sdc_builder, sdc_executor):
    pass

