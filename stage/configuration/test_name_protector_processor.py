import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'anonymize_by': 'CLASSIFICATION_RESULT'},
                                              {'anonymize_by': 'FIELD'},
                                              {'anonymize_by': 'TYPE'}])
def test_anonymize_by(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'anonymize_by': 'CLASSIFICATION_RESULT'}])
def test_category_pattern(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'anonymize_by': 'FIELD'}])
def test_field_paths(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'anonymize_by': 'CLASSIFICATION_RESULT'}])
def test_score_threshold(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{}, {'anonymize_by': 'TYPE', 'source_type': 'STRING'}])
def test_source_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'transformation': 'ABBREVIATE'}, {'transformation': 'FIRSTONLY'}])
def test_transformation(sdc_builder, sdc_executor, stage_attributes):
    pass

