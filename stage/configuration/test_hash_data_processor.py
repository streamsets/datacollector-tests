import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'algorithm': 'MD2'},
                                              {'algorithm': 'MD5'},
                                              {'algorithm': 'SHA_1'},
                                              {'algorithm': 'SHA_256'},
                                              {'algorithm': 'SHA_384'},
                                              {'algorithm': 'SHA_512'}])
def test_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'append_salt': False}, {'append_salt': True}])
def test_append_salt(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'base64_decode_input': False}, {'base64_decode_input': True}])
def test_base64_decode_input(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'base64_encode_output': False}, {'base64_encode_output': True}])
def test_base64_encode_output(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'append_salt': True}])
def test_salt(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'append_salt': True, 'salt_is_base64_encoded': False},
                                              {'append_salt': True, 'salt_is_base64_encoded': True}])
def test_salt_is_base64_encoded(sdc_builder, sdc_executor, stage_attributes):
    pass

