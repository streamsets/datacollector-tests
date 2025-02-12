# Copyright 2025 StreamSets Inc.
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

import pytest
from streamsets.testframework.markers import sdc_min_version

@sdc_min_version('6.2.0')
def test_jython_field_attributes(sdc_builder, sdc_executor):
    """Test that Javascript evaluator preserves field header attributes

    The pipeline would look like:

        dev_raw_data_source >> expression_evaluator >> javascript_evaluator >> wiretap.destination
    """
    utc_datetime_str = '1978-01-05 19:38:01'
    raw_str_value = 'hello again!'
    # note, date time here is in UTC. Each map is an SDC record to process.
    raw_col = [{'amInteger': 123, 'amDouble': 12345.6789115, 'amString': 'hello', 'amBool': True, 'amDateTime': utc_datetime_str, 'amString2': raw_str_value, 'amZonedDateTime': None}]
    raw_data = json.dumps(raw_col)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(field_attribute_expressions=[
        {
            "fieldToSet": "/amInteger",
            "attributeToSet": "amInteger",
            "fieldAttributeExpression": "${\"amInteger\"}"
        },{
            "fieldToSet": "/amDouble",
            "attributeToSet": "amDouble",
            "fieldAttributeExpression": "${\"amDouble\"}"
        },{
            "fieldToSet": "/amString",
            "attributeToSet": "amString",
            "fieldAttributeExpression": "${\"amString\"}"
        },{
            "fieldToSet": "/amBool",
            "attributeToSet": "amBool",
            "fieldAttributeExpression": "${\"amBool\"}"
        },{
            "fieldToSet": "/amDateTime",
            "attributeToSet": "amDateTime",
            "fieldAttributeExpression": "${\"amDateTime\"}"
        },{
            "fieldToSet": "/amString2",
            "attributeToSet": "amString2",
            "fieldAttributeExpression": "${\"amString2\"}"
        },{
            "fieldToSet": "/amZonedDateTime",
            "attributeToSet": "amZonedDateTime",
            "fieldAttributeExpression": "${\"amZonedDateTime\"}"
        },
    ])

    javascript_evaluator = pipeline_builder.add_stage('JavaScript Evaluator', type='processor')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> expression_evaluator >> javascript_evaluator >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # assert data type conversion
    record = wiretap.output_records[0]

    assert record.field['amInteger'].value == 123
    assert record.field['amDouble'].value == 12345.6789115
    assert record.field['amString'].value == 'hello'
    assert record.field['amBool'].value == True
    assert record.field['amDateTime'].value == utc_datetime_str
    assert record.field['amString2'].value == raw_str_value
    assert record.field['amZonedDateTime'].value is None

    assert str(record.get_field_attributes('amInteger')) == "{'amInteger': 'amInteger'}"
    assert str(record.get_field_attributes('amDouble')) == "{'amDouble': 'amDouble'}"
    assert str(record.get_field_attributes('amString')) == "{'amString': 'amString'}"
    assert str(record.get_field_attributes('amBool')) == "{'amBool': 'amBool'}"
    assert str(record.get_field_attributes('amDateTime')) == "{'amDateTime': 'amDateTime'}"
    assert str(record.get_field_attributes('amString2')) == "{'amString2': 'amString2'}"
    assert str(record.get_field_attributes('amZonedDateTime')) == "{'amZonedDateTime': 'amZonedDateTime'}"


@sdc_min_version('6.2.0')
@pytest.mark.parametrize('record_processing_mode', ['BATCH', 'RECORD'])
@pytest.mark.parametrize('script_error_as_record_error', [False, True])
@pytest.mark.parametrize('on_error_record', ['DISCARD', 'TO_ERROR', 'STOP_PIPELINE'])
def test_script_errors_behavior(
        sdc_builder,
        sdc_executor,
        record_processing_mode,
        script_error_as_record_error,
        on_error_record
):
    """Testing stage behavior when the script processor raise an error"""
    builder = sdc_builder.get_pipeline_builder()

    raw_data = '{}'

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    javascript_evaluator = builder.add_stage('JavaScript Evaluator', type='processor')
    javascript_evaluator.script = 'throw "Custom error"'
    javascript_evaluator.script_error_as_record_error = script_error_as_record_error
    javascript_evaluator.on_record_error = on_error_record
    javascript_evaluator.record_processing_mode = record_processing_mode

    wiretap = builder.add_wiretap()

    origin >> javascript_evaluator >> wiretap.destination

    pipeline = builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    except:
        if not script_error_as_record_error or (script_error_as_record_error and on_error_record == "STOP_PIPELINE"):
            pipeline_final_status = sdc_executor.get_pipeline_status(pipeline).response.json()
            status = pipeline_final_status['status']
            message = pipeline_final_status['attributes']['ERROR_CODE']
            stacktrace = pipeline_final_status['attributes']['ERROR_STACKTRACE']
            assert 'RUN_ERROR' == status
            if record_processing_mode == 'BATCH':
                assert 'SCRIPTING_06' == message
            else:
                assert 'SCRIPTING_05' == message
            assert 'Custom error' in stacktrace
        else:
            pytest.fail("This pipeline should not fail")
    else:
        if (record_processing_mode == 'RECORD' or script_error_as_record_error) and on_error_record == "TO_ERROR":
            error_records = wiretap.error_records
            assert len(error_records) == 1
            if record_processing_mode == 'BATCH':
                assert error_records[0].header['errorCode'] == 'SCRIPTING_06'
            else:
                assert error_records[0].header['errorCode'] == 'SCRIPTING_05'
            stacktrace = error_records[0].header['errorStackTrace']
            assert 'Custom error' in stacktrace
        elif (record_processing_mode == 'RECORD' or script_error_as_record_error) and on_error_record == "DISCARD":
            assert len(wiretap.error_records) == 0
        else:
            pytest.fail("This pipeline should fail")
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
