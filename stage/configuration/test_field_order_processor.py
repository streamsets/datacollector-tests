# Copyright 2020 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from collections import OrderedDict

import dateutil.parser
import pytest

# A dictionary that maps default data types to default values
DEFAULT_TYPE_TO_VALUE = {
    'BOOLEAN': 'True',
    'DATE': '2019-11-11T12:00Z',
    'DATETIME': '2019-11-11T12:00Z',
    'DECIMAL': '2.0',
    'DOUBLE': '2.0',
    'FLOAT': '2.0',
    'INTEGER': '10',
    'LONG': '1000',
    'SHORT': '255',
    'STRING': 'Default Value',
    'TIME': '2019-11-11T12:00Z'
}


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
    """Tests the default_type configuration.  This function in turn calls the
    test_missing_fields test case because the default_type configuration
    parameter is only applicable when missing fields is set to USE_DEFAULT
    """
    stage_attributes['default_value'] = DEFAULT_TYPE_TO_VALUE[stage_attributes['default_type']]
    test_missing_fields(sdc_builder, sdc_executor, stage_attributes)


@pytest.mark.parametrize('stage_attributes', [{'missing_fields': 'USE_DEFAULT'}])
def test_default_value(sdc_builder, sdc_executor, stage_attributes):
    """Tests the default_value configuration.  This function in turn calls the
    test_default_type test case by specifying a specific default_type (in this
    case a STRING).  The value itself is picked up in the test_default_type
    test case.
    """
    stage_attributes['default_type'] = 'STRING'
    test_default_type(sdc_builder, sdc_executor, stage_attributes)


@pytest.mark.parametrize('stage_attributes', [{'extra_fields': 'TO_ERROR'}])
def test_discard_fields(sdc_builder, sdc_executor, stage_attributes):
    """Tests the discard_fields configuration property.  If a discarded field is
    specified, then it is removed from the output.  This option is only
    relevant when the extra_fields configuration property is set to TO_ERROR,
    otherwise all extra fields are discarded.
    """
    data = {'f1': 'f1', 'f2': 'f2', 'f3': 'f3'}
    expected_data = OrderedDict([('f3', 'f3'), ('f2', 'f2')])

    stage_attributes.update(dict(discard_fields=['/f1'],
                                 fields_to_order=['/f3', '/f2']))

    wiretap = _get_pipeline_records(sdc_builder, sdc_executor, data, stage_attributes)
    assert [record.field for record in wiretap.output_records] == [expected_data]


@pytest.mark.parametrize('stage_attributes', [{'extra_fields': 'DISCARD'}, {'extra_fields': 'TO_ERROR'}])
def test_extra_fields(sdc_builder, sdc_executor, stage_attributes):
    """Tests the extra_fields configuration property. The two options are to
    either discard all the extra fields or to error out any records that have
    extra fields. Depending on the input, the test either checks to see if
    there are any error records or if the stage has properly discarded the
    extra field from the output record.
    """
    data = {'f1': 'f1', 'f2': 'f2', 'f3': 'f3'}
    expected_data = OrderedDict([('f3', 'f3'), ('f2', 'f2')])

    stage_attributes['fields_to_order'] = ['/f3', '/f2']
    wiretap = _get_pipeline_records(sdc_builder, sdc_executor, data, stage_attributes)

    if stage_attributes['extra_fields'] == 'TO_ERROR':
        assert len(wiretap.error_records) == 1
    else:
        assert [record.field for record in wiretap.output_records] == [expected_data]


def test_fields_to_order(sdc_builder, sdc_executor):
    """Test the most basic configuration of the field order processor stage.
    The tests verifies that the order provided as part of the config is exactly
    how the output of the stage is generated.
    """
    data = {'f1': 'f1', 'f2': 'f2', 'f3': 'f3'}
    stage_attributes = {'fields_to_order': ['/f1', '/f3', '/f2']}
    expected_data = OrderedDict([('f1', 'f1'), ('f3', 'f3'), ('f2', 'f2')])

    wiretap = _get_pipeline_records(sdc_builder, sdc_executor, data, stage_attributes)
    assert [record.field for record in wiretap.output_records] == [expected_data]


@pytest.mark.parametrize('stage_attributes', [{'missing_fields': 'TO_ERROR'}, {'missing_fields': 'USE_DEFAULT'}])
def test_missing_fields(sdc_builder, sdc_executor, stage_attributes):
    """Tests the missing_fields configuration. If missing_fields is set to USE_DEFAULT,
    them make sure that there is a default_type and default_value setup.  These values
    can already be set by another test case (test_default_type or test_default_value).  If they
    aren't already set, set them here to avoid a pipeline validation error.
    """
    data = {'f1': 'f1', 'f2': 'f2', 'f3': 'f3'}
    if ((stage_attributes['missing_fields'] == 'USE_DEFAULT') and
            ('default_type' not in stage_attributes or 'default_value' not in stage_attributes)):
        stage_attributes.update(dict(default_type='STRING',
                                     default_value=DEFAULT_TYPE_TO_VALUE['STRING']))

    stage_attributes['fields_to_order'] = ['/f1', '/f2', '/f3', '/f4']

    wiretap = _get_pipeline_records(sdc_builder, sdc_executor, data, stage_attributes)
    if stage_attributes['missing_fields'] == 'USE_DEFAULT':
        assert [record.field['f4'].type for record in wiretap.output_records] == [stage_attributes['default_type']]

        default_input_value = [stage_attributes['default_value']]
        default_output_value = [str(record.field['f4'].value) for record in wiretap.output_records]

        # If the default data type is a DATE, DATETIME or TIME, just comparing
        # strings doesn't really work.  You'll need to convert to the
        # appropriate datetime objects with the correct timezone and then
        # compare.
        if stage_attributes['default_type'] in ['DATE', 'DATETIME', 'TIME']:
            default_output_value = [dateutil.parser.parse(f"{record.field['f4'].value.isoformat()}Z")
                                    for record in wiretap.output_records]
            default_input_value = [dateutil.parser.parse(stage_attributes['default_value'])]
        assert default_output_value == default_input_value
    else:
        assert len(wiretap.error_records) == 1


@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    """Tests the on_record_error configuration. The configuration in this test always
    generates an error when processing the record.  Depending on what the config for
    on_record_error is, the tests checks for validates different outputs.
    """
    source_data = {'f1': 'f1', 'f2': 'f2', 'f3': 'f3'}
    stage_attributes['missing_fields'] = 'TO_ERROR'

    # the extra '/f4' will generate a record processing error.
    stage_attributes['fields_to_order'] = ['/f1', '/f2', '/f3', '/f4']

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON',
            raw_data=json.dumps(source_data),
            stop_after_first_batch=True
        )
        pipeline_builder.add_error_stage('Discard')
        field_order = pipeline_builder.add_stage('Field Order').set_attributes(**stage_attributes)
        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source >> field_order >> wiretap.destination
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # if on_record_error is DISCARD, check that output and error counts are
        # 0.  If on_record_error is TO_ERROR, then error counts should be 1
        if stage_attributes['on_record_error'] == 'DISCARD':
            assert len(wiretap.output_records) == 0
            assert len(wiretap.error_records) == 0
        elif stage_attributes['on_record_error'] == 'TO_ERROR':
            assert len(wiretap.error_records) == 1
        else:
            # The third condition is when on_record_error is set to
            # STOP_PIPELINE, in which case there should be an exception
            assert False

    except:
        # If the on_record_error is set to STOP_PIPELINE, the pipeline is
        # stopped and an exception is thrown.  Check to see if this is a
        # RUN_ERROR. If an exception is thrown when on_record_error is not
        # STOP_PIPELINE, then trigger a failure.
        status_message = sdc_executor.get_pipeline_status(pipeline).response.json()['status']
        test_succeeded = (status_message == 'RUN_ERROR') if (stage_attributes['on_record_error'] ==
                                                             'STOP_PIPELINE') else False
        assert test_succeeded


@pytest.mark.parametrize('stage_attributes', [{'output_type': 'LIST'}, {'output_type': 'LIST_MAP'}])
def test_output_type(sdc_builder, sdc_executor, stage_attributes):
    """Tests the output_type configuration.  Depending on the configuration,
    the tests checks for a list or an OrderedDictionary as the output data type
    """
    data = {'f1': 'f1', 'f2': 'f2', 'f3': 'f3'}
    expected_data_type = list if (stage_attributes['output_type'] ==
                                  'LIST') else OrderedDict

    stage_attributes['fields_to_order'] = ['/f3', '/f2', '/f1']
    wiretap = _get_pipeline_records(sdc_builder, sdc_executor, data, stage_attributes)

    assert [type(record.field) for record in wiretap.output_records] == [expected_data_type]


def test_preconditions(sdc_builder, sdc_executor):
    """Tests the preconditions configuration.  This is setup to fail the
    record processing due to a failed pre-condition.  Check the error_records count.
    """
    data = {'f1': 'f1', 'f2': 'f2', 'f3': 'f3'}
    stage_attributes = {
        'fields_to_order': ['/f3', '/f2', '/f1'],
        'preconditions': ["${record:value('/f1') == 'f2'}"]
    }
    wiretap = _get_pipeline_records(sdc_builder, sdc_executor, data, stage_attributes)
    assert len(wiretap.error_records) == 1


def test_required_fields(sdc_builder, sdc_executor):
    """Tests the required_fields configuration.  This test is setup to fail by
    adding a required field which is not present in the input data set.
    """
    data = {'f1': 'f1', 'f2': 'f2', 'f3': 'f3'}
    stage_attributes = {
        'fields_to_order': ['/f1', '/f3', '/f2'],
        'required_fields': ['/f4']
    }

    wiretap = _get_pipeline_records(sdc_builder, sdc_executor, data, stage_attributes)

    # The one processed record should trigger an error since
    # a required field is missing in the source data
    assert len(wiretap.error_records) == 1


def _get_pipeline_records(sdc_builder, sdc_executor, source_data, stage_attributes):
    """A utility function that creates a pipeline which includes a dev raw data
    source, a field order processor.  The destination is Wiretap. The
    pipeline is executed and the wiretap output is returned.
    The pipeline runs for only one batch of input and is removed once
    completed. This function is used by all other test functions in this module
    except the test_on_record_error test case which needs some special
    handling.
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage(
        'Dev Raw Data Source').set_attributes(data_format='JSON',
                                              raw_data=json.dumps(source_data),
                                              stop_after_first_batch=True)
    pipeline_builder.add_error_stage('Discard')
    field_order = pipeline_builder.add_stage('Field Order').set_attributes(**stage_attributes)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_order >> wiretap.destination
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    return wiretap
