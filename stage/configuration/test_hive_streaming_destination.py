import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'create_partitions': False}, {'create_partitions': True}])
def test_create_partitions(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_to_column_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_hive_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_hive_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
def test_hive_metastore_thrift_url(sdc_builder, sdc_executor):
    pass


@stub
def test_max_record_size_in_kb(sdc_builder, sdc_executor):
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
def test_schema(sdc_builder, sdc_executor):
    pass


@stub
def test_table(sdc_builder, sdc_executor):
    pass


@stub
def test_transaction_batch_size(sdc_builder, sdc_executor):
    pass

