import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'default_type': 'BOOLEAN', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'DATE', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'DATETIME', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'DECIMAL', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'DOUBLE', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'FLOAT', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'INTEGER', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'LONG', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'SHORT', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'STRING', 'missing_fields': 'USE_DEFAULT'},
                                              {'default_type': 'TIME', 'missing_fields': 'USE_DEFAULT'}])
def test_default_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_fields': 'USE_DEFAULT'}])
def test_default_value(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'extra_fields': 'TO_ERROR'}])
def test_discard_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'extra_fields': 'DISCARD'}, {'extra_fields': 'TO_ERROR'}])
def test_extra_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_fields_to_order(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_fields': 'TO_ERROR'}, {'missing_fields': 'USE_DEFAULT'}])
def test_missing_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'output_type': 'LIST'}, {'output_type': 'LIST_MAP'}])
def test_output_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

