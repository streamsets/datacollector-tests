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
@pytest.mark.parametrize('stage_attributes', [{'data_type': 'BOOLEAN'},
                                              {'data_type': 'BYTE'},
                                              {'data_type': 'CHAR'},
                                              {'data_type': 'DATE'},
                                              {'data_type': 'DATETIME'},
                                              {'data_type': 'DECIMAL'},
                                              {'data_type': 'DOUBLE'},
                                              {'data_type': 'FLOAT'},
                                              {'data_type': 'INTEGER'},
                                              {'data_type': 'LONG'},
                                              {'data_type': 'SHORT'},
                                              {'data_type': 'STRING'},
                                              {'data_type': 'TIME'},
                                              {'data_type': 'ZONED_DATETIME'}])
def test_data_type(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{},
                                              {'anonymize_by': 'TYPE', 'source_type': 'BOOLEAN'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'BYTE'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'BYTE_ARRAY'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'CHAR'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'DATE'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'DATETIME'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'DECIMAL'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'DOUBLE'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'FILE_REF'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'FLOAT'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'INTEGER'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'LONG'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'SHORT'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'STRING'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'TIME'},
                                              {'anonymize_by': 'TYPE', 'source_type': 'ZONED_DATETIME'}])
def test_source_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_value(sdc_builder, sdc_executor):
    pass

