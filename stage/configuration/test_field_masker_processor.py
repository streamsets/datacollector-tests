import json

import pytest
from streamsets.testframework.decorators import stub


def test_field_mask_configs(sdc_builder, sdc_executor):
    """Test field mask configs.

     Input           | Mask Type          | Output
    -----------------|--------------------|------------------
     donKey          | Fixed length       | xxxxxxxxxx
     donKey          | Variable length    | xxxxxx
     617-567-8888    | Custom             | 617-xxx-xxxx
     123-45-6789     | Regular Expression | xxxx45xxxxx
    ---------------------------------------------------------
    """
    try:
        DATA = dict(password1='donKey', password2='donKey', phoneNumber='617-567-8888', ssn='123-45-6789')
        EXPECTED_MASKED_DATA = dict(password1='xxxxxxxxxx', password2='xxxxxx',
                                    phoneNumber='617-xxx-xxxx', ssn='xxxx45xxxxx')

        field_mask_configs = [{'fields': ['/password1'],
                               'maskType': 'FIXED_LENGTH',
                               'regex': '(.*)',
                               'groupsToShow': '1'},
                              {'fields': ['/password2'],
                               'maskType': 'VARIABLE_LENGTH',
                               'regex': '(.*)',
                               'groupsToShow': '1'},
                              {'fields': ['/phoneNumber'],
                               'maskType': 'CUSTOM',
                               'regex': '(.*)',
                               'groupsToShow': '1',
                               'mask': '###-xxx-xxxx'},
                              {'fields': ['/ssn'],
                               'maskType': 'REGEX',
                               'regex': '([0-9]{3})-([0-9]{2})-([0-9]{4})',
                               'groupsToShow': '2'}]

        pipeline_builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps(DATA))
        field_masker = pipeline_builder.add_stage('Field Masker').set_attributes(field_mask_configs=field_mask_configs)
        trash = pipeline_builder.add_stage('Trash')

        dev_raw_data_source >> field_masker >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        record = snapshot[field_masker].output[0]
        assert record.field == EXPECTED_MASKED_DATA
    finally:
        sdc_executor.stop_pipeline(pipeline)


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

