import pytest

from streamsets.testframework.decorators import stub


@stub
def test_database_field_mappings(sdc_builder, sdc_executor):
    pass


@stub
def test_geoip2_databases(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_address_action': 'IGNORE'},
                                              {'missing_address_action': 'REPLACE_WITH_NULLS'},
                                              {'missing_address_action': 'TO_ERROR'}])
def test_missing_address_action(sdc_builder, sdc_executor, stage_attributes):
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

