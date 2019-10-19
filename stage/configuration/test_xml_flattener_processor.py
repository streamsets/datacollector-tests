import pytest

from streamsets.testframework.decorators import stub


@stub
def test_attribute_delimiter(sdc_builder, sdc_executor):
    pass


@stub
def test_field_delimiter(sdc_builder, sdc_executor):
    pass


@stub
def test_field_to_flatten(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_attributes': False}, {'ignore_attributes': True}])
def test_ignore_attributes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_namespace_uri': False}, {'ignore_namespace_uri': True}])
def test_ignore_namespace_uri(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keep_original_fields': False}, {'keep_original_fields': True}])
def test_keep_original_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keep_original_fields': True}])
def test_output_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keep_original_fields': True, 'overwrite_existing_fields': False},
                                              {'keep_original_fields': True, 'overwrite_existing_fields': True}])
def test_overwrite_existing_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_record_delimiter(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

