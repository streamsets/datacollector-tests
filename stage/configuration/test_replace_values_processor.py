import pytest

from streamsets.testframework.decorators import stub


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
def test_value(sdc_builder, sdc_executor):
    pass

