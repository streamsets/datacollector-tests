import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'email': 'CUSTOM'}])
def test_custom_format_for_email(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'us_phone': 'CUSTOM'}])
def test_custom_format_for_us_phone(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'us_ssn': 'CUSTOM'}])
def test_custom_format_for_us_ssn(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'us_zip_code': 'CUSTOM'}])
def test_custom_format_for_us_zip_code(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'email': 'CUSTOM'}, {'email': 'DOMAIN_SHORT'}, {'email': 'LOCAL_SHORT'}])
def test_email(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'us_phone': 'CUSTOM'}, {'us_phone': 'LINE_NUMBER'}])
def test_us_phone(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'us_ssn': 'CUSTOM'}, {'us_ssn': 'XXX_XX_NNNN'}])
def test_us_ssn(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'us_zip_code': 'CUSTOM'},
                                              {'us_zip_code': 'PREFIX_ONLY'},
                                              {'us_zip_code': 'SUFFIX_ONLY'}])
def test_us_zip_code(sdc_builder, sdc_executor, stage_attributes):
    pass

