import pytest

from streamsets.testframework.decorators import stub


@stub
def test_application_name_in_standalone_mode_only(sdc_builder, sdc_executor):
    pass


@stub
def test_init_method_arguments(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_parallelism_in_standalone_mode_only(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_spark_transformer_class(sdc_builder, sdc_executor):
    pass

