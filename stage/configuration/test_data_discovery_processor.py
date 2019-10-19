import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'classification_engine': 'CUSTOM_PATTERNS'},
                                              {'classification_engine': 'STREAMSETS'}])
def test_classification_engine(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'scope': 'SPECIFIED_FIELDS'}])
def test_field_paths(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'sampling': 'RANDOM_SAMPLE'}])
def test_generation_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'classification_engine': 'CUSTOM_PATTERNS'}])
def test_patterns(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'result_aggregation_method': 'MAX'},
                                              {'result_aggregation_method': 'MEAN'},
                                              {'result_aggregation_method': 'MEDIAN'},
                                              {'result_aggregation_method': 'MIN'}])
def test_result_aggregation_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'sampling': 'RANDOM_SAMPLE'}])
def test_sample_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'sampling': 'ALL_RECORDS'},
                                              {'sampling': 'FIRST_BATCH_RECORD'},
                                              {'sampling': 'NEW_PATHS'},
                                              {'sampling': 'RANDOM_SAMPLE'}])
def test_sampling(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'scope': 'ALL_FIELDS'}, {'scope': 'SPECIFIED_FIELDS'}])
def test_scope(sdc_builder, sdc_executor, stage_attributes):
    pass

