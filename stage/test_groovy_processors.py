# Copyright 2017 StreamSets Inc.
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
import logging

import pytest
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, redefined-outer-name, too-many-locals

# Definition of various groovy libraries and since when they were included in Data Collector
GROOVY_LIBS = [
  ("2.4", "streamsets-datacollector-groovy_2_4-lib", Version("1.0")),
  ("4.0", "streamsets-datacollector-groovy_4_0-lib", Version("5.2.0"))
]


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
      sdc_version = Version(data_collector.version)

      for info in GROOVY_LIBS:
        if sdc_version >= info[2]:
          data_collector.add_stage_lib(info[1])

    return hook


@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
def test_groovy_evaluator(sdc_builder, sdc_executor, groovy_version, library, min_sdc_version):
    """Test Groovy Evaluator processor. We test by setting up a template object in initialization script, then
    main processing script which access the template to create a Groovy object per record and to create a new record
    attribute. Finally in destroy script, we collate all the records for its new attribute and Groovy assert its value.
    The pipeline would look like:

        dev_raw_data_source >> groovy_evaluator >> wiretap
    """
    if Version(sdc_builder.version) < min_sdc_version:
      pytest.skip(f"Data Collector {sdc_builder.version} doesn't support Groovy {groovy_version}")

    raw_company_1 = dict(name='StreamSets', floors=3)
    raw_company_2 = dict(name='Example Inc.', floors=1)
    raw_company_3 = dict(name='ASDF', floors=10)
    raw_company_4 = dict(name='QWER Inc.', floors=2)
    raw_data = json.dumps([raw_company_1, raw_company_2, raw_company_3, raw_company_4])

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    groovy_evaluator = pipeline_builder.add_stage('Groovy Evaluator', type='processor', library=library)
    # in the init script we create a 'Building' template object which can be cloned per each pipeline record processing
    init_script = """
        import groovy.transform.*

        @AutoClone(style=AutoCloneStyle.COPY_CONSTRUCTOR)
        class Building implements Cloneable{
            String name
            int floors
            boolean officeSpace
        }

        state['building_template'] = new Building()
    """
    # in the script for every record processing, we compute if a building has more than 2 floors and
    # if so we create a record attribute 'officeSpace' to 'true' else 'false'
    script = """
        state['buildings'] = []
        for (record in records) {
          try {
            record.value['officeSpace'] = (record.value['floors'] > 2) ?: false

            def building = state['building_template'].clone()
            building.name = record.value['name']
            building.floors = record.value['floors']
            building.officeSpace = record.value['officeSpace']
            state['buildings'] << building
            output.write(record)
          } catch (e) {
            log.error(e.toString(), e)
            error.write(record, e.toString())
          }
        }
    """
    # in the destroy script, we collate all the building names which have 'officeSpace' and do a Groovy assert
    destroy_script = """
        def buildings = state['buildings']
        def mapBuildingName = { building -> building.name }
        def officeBuildingNames =
            buildings
                .stream()
                .filter { building ->
                    building.officeSpace && building.floors > 2
                }
                .map(mapBuildingName)
                .collect()

        assert officeBuildingNames == ['StreamSets', 'ASDF']
    """
    groovy_evaluator.set_attributes(destroy_script=destroy_script, enable_invokedynamic_compiler_option=True,
                                    init_script=init_script, record_processing_mode='BATCH', script=script)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> groovy_evaluator >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # search for a record whose 'name' is raw_company_1['name'] and assert new attribute ('officeSpace') is created
    # with expected boolean value (where 'floors' > 2)
    record_1 = next(record for record in wiretap.output_records if record.field['name'] == raw_company_1['name'])
    assert record_1.field['officeSpace'] == (raw_company_1['floors'] > 2)
    record_2 = next(record for record in wiretap.output_records if record.field['name'] == raw_company_2['name'])
    assert record_2.field['officeSpace'] == (raw_company_2['floors'] > 2)


# SDC-10353: Unable to delete record headers in Scripting Evaluators
@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
def test_delete_header_attribute(sdc_builder, sdc_executor, groovy_version, library, min_sdc_version):
    """Make sure that deleted attributes stays deleted."""
    if Version(sdc_builder.version) < min_sdc_version:
      pytest.skip(f"Data Collector {sdc_builder.version} doesn't support Groovy {groovy_version}")

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='TEXT', raw_data="BLA BLA", stop_after_first_batch=True)

    expression = builder.add_stage('Expression Evaluator')
    expression.header_attribute_expressions = [
        {'attributeToSet': 'remove', 'headerAttributeExpression': 'I should be deleted'}
    ]

    groovy = builder.add_stage('Groovy Evaluator', library=library)
    groovy.init_script = ''
    groovy.destroy_script = ''
    groovy.script =  """
        for (record in records) {
            record.attributes.remove('remove')
            output.write(record)
        }
    """

    wiretap = builder.add_wiretap()

    origin >> expression >> groovy >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert 'remove' not in wiretap.output_records[0].header


# SDC-11546: Expose the underlying Data Collector Record in Scripting processors
@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
@sdc_min_version('3.9.0')
def test_expose_sdc_record(sdc_builder, sdc_executor, groovy_version, library, min_sdc_version):
    """Ensure that underlying SDC record is accessible."""
    if Version(sdc_builder.version) < min_sdc_version:
      pytest.skip(f"Data Collector {sdc_builder.version} doesn't support Groovy {groovy_version}")

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='TEXT', raw_data="BLA BLA", stop_after_first_batch=True)

    expression = builder.add_stage('Expression Evaluator')
    expression.field_attribute_expressions = [
        {"fieldToSet": "/text", "attributeToSet": "attr", "fieldAttributeExpression": "is-here"}
    ]

    groovy = builder.add_stage('Groovy Evaluator', library=library)
    groovy.init_script = ''
    groovy.destroy_script = ''
    groovy.script =  """
        for (record in records) {
            record.attributes['attr'] = record.sdcRecord.get('/text').getAttribute('attr')
            output.write(record)
        }
    """

    wiretap = builder.add_wiretap()

    origin >> expression >> groovy >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    # STF-798: RecordHeader is inconsistent in STF and SDC
    assert 'is-here' == wiretap.output_records[0].header['values']['attr']


# SDC-11555: Provide ability to use direct SDC record in scripting processors
@sdc_min_version('3.9.0')
@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
def test_sdc_record(sdc_builder, sdc_executor, groovy_version, library, min_sdc_version):
    """Iterate over SDC record directly rather then JSR-223 wrapper."""
    if Version(sdc_builder.version) < min_sdc_version:
      pytest.skip(f"Data Collector {sdc_builder.version} doesn't support Groovy {groovy_version}")

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{"old": "old-value"}')
    origin.stop_after_first_batch = True

    groovy = builder.add_stage('Groovy Evaluator', library=library)
    groovy.record_type = 'SDC_RECORDS'
    groovy.init_script = ''
    groovy.destroy_script = ''
    groovy.script =  """
        import com.streamsets.pipeline.api.Field
        for (record in records) {
            record.sdcRecord.set('/new', Field.create(Field.Type.STRING, 'new-value'))
            record.sdcRecord.get('/old').setAttribute('attr', 'attr-value')
            output.write(record)
        }
    """

    wiretap = builder.add_wiretap()

    origin >> groovy >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert 'new-value' == wiretap.output_records[0].field['new']
    assert 'attr-value' == wiretap.output_records[0].get_field_data('/old').attributes['attr']


@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
def test_event_creation(sdc_builder, sdc_executor, groovy_version, library, min_sdc_version):
    """Ensure that the process is able to create events."""
    builder = sdc_builder.get_pipeline_builder()
    if Version(sdc_builder.version) < min_sdc_version:
      pytest.skip(f"Data Collector {sdc_builder.version} doesn't support Groovy {groovy_version}")

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{"old": "old-value"}')
    origin.stop_after_first_batch = True

    groovy = builder.add_stage('Groovy Evaluator', library=library)
    groovy.init_script = ''
    groovy.destroy_script = ''
    groovy.script =  """
event = sdcFunctions.createEvent("event", 1)
event.value = sdcFunctions.createMap(true)
event.value['value'] = "secret"
sdcFunctions.toEvent(event)
"""

    records_wiretap = builder.add_wiretap()
    events_wiretap = builder.add_wiretap()

    origin >> groovy >> records_wiretap.destination
    groovy >= events_wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(events_wiretap.output_records) == 1
    assert events_wiretap.output_records[0].get_field_data('/value') == 'secret'


@sdc_min_version('4.1.0')
@pytest.mark.parametrize('on_script_error', [(True, 'STOP_PIPELINE'),
                                             (True, 'TO_ERROR'),
                                             (False, 'TO_ERROR')])
@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
def test_script_error(sdc_builder, sdc_executor, on_script_error, groovy_version, library, min_sdc_version):
    """Evaluate batches with 5 records and write 4 of them and send the first one to error. Check if pipeline stops or
    send the error to log depends on Groovy Evaluator configuration. The pipeline looks like:
        groovy_origin >> groovy_evaluator >> wiretap.destination
    """
    if Version(sdc_builder.version) < min_sdc_version:
      pytest.skip(f"Data Collector {sdc_builder.version} doesn't support Groovy {groovy_version}")

    batch_size = 5
    pipeline_builder = sdc_builder.get_pipeline_builder()
    script_origin = """
        entityName = ''
        offset = 0
        cur_batch = sdc.createBatch()
        cur_batch = sdc.createBatch()
        record = sdc.createRecord('generated data')
        
        hasNext = true
        while (hasNext) {
            try {
                offset = offset + 1
                record = sdc.createRecord('generated data')
                value = entityName + ':' + offset.toString()
                record.value = value
                cur_batch.add(record)
        
                // if the batch is full, process it and start a new one
                if (cur_batch.size() >= sdc.batchSize) {
                    cur_batch.process(entityName, offset.toString())
                    cur_batch = sdc.createBatch()
                    if (sdc.isStopped()) {
                        hasNext = false
                    }
                  count = 0
                }
            } catch (Exception e) {
                cur_batch.addError(record, e.toString())
                cur_batch.process(entityName, offset.toString())
                hasNext = false
            }
        }
        
        if (cur_batch.size() + cur_batch.errorCount() + cur_batch.eventCount() > 0) {
            cur_batch.process(entityName, offset.toString())
        }
        """

    groovy_origin = pipeline_builder.add_stage('Groovy Scripting', library=library)
    groovy_origin.set_attributes(record_type='NATIVE_OBJECTS',
                                 user_script=script_origin,
                                 batch_size=batch_size,
                                 number_of_threads=1)

    groovy_evaluator = pipeline_builder.add_stage('Groovy Evaluator', type='processor')
    script_processor = '''
        records = sdc.records
        count = 1
        for (record in records) {
            try {
              if (count == 1){
                sdc.error.write(record, 'Groovy STF force error')
              } else {
                sdc.output.write(record)
              }
              count = count + 1
            } catch (Exception e) {
                // Write a record to the error pipeline
                sdc.log.error(e.toString(), e)
                sdc.error.write(record, e.toString())
            }
        }
        '''
    groovy_evaluator.set_attributes(record_processing_mode='BATCH',
                                    script=script_processor,
                                    init_script='',
                                    destroy_script='',
                                    script_error_as_record_error=on_script_error[0],
                                    on_record_error=on_script_error[1])

    wiretap = pipeline_builder.add_wiretap()

    groovy_origin >> groovy_evaluator >> wiretap.destination

    pipeline = pipeline_builder.build()
    pipeline.configuration['runnerIdleTIme'] = 2
    pipeline.configuration['shouldRetry'] = False

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        total_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

        # Since the batch size was 5, the output records will be 1 error + 4 output
        expected_error_records = int(total_records/batch_size)
        assert len(wiretap.error_records) == expected_error_records
        assert len(wiretap.output_records) == total_records - expected_error_records
    except Exception as e:
        assert len(wiretap.error_records) == 0
        assert 'SCRIPTING_06' in e.message


@sdc_min_version('6.2.0')
@pytest.mark.parametrize('record_processing_mode', ['BATCH', 'RECORD'])
@pytest.mark.parametrize('script_error_as_record_error', [False, True])
@pytest.mark.parametrize('on_error_record', ['DISCARD', 'TO_ERROR', 'STOP_PIPELINE'])
def test_script_errors_behavior(
        sdc_builder, sdc_executor,
        record_processing_mode,
        script_error_as_record_error,
        on_error_record
):
    """Testing stage behavior when the script processor raise an error"""
    builder = sdc_builder.get_pipeline_builder()

    raw_data = '{}'

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    groovy_evaluator = builder.add_stage('Groovy Evaluator', type='processor')
    groovy_evaluator.script = 'throw new Exception("Custom error")'
    groovy_evaluator.script_error_as_record_error = script_error_as_record_error
    groovy_evaluator.on_record_error = on_error_record
    groovy_evaluator.record_processing_mode = record_processing_mode

    wiretap = builder.add_wiretap()

    origin >> groovy_evaluator >> wiretap.destination

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
            assert 'java.lang.Exception' in stacktrace
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
            assert 'java.lang.Exception' in stacktrace
        elif (record_processing_mode == 'RECORD' or script_error_as_record_error) and on_error_record == "DISCARD":
            assert len(wiretap.error_records) == 0
        else:
            pytest.fail("This pipeline should fail")
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('6.2.0')
@pytest.mark.parametrize('groovy_version,library,min_sdc_version', GROOVY_LIBS, ids=[i[0] for i in GROOVY_LIBS])
def test_groovy_field_attributes(sdc_builder, sdc_executor, groovy_version, library, min_sdc_version):
    """Test that Groovy evaluator preserves field header attributes

    The pipeline would look like:

        dev_raw_data_source >> expression_evaluator>> groovy_evaluator >> wiretap
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

    groovy_evaluator = pipeline_builder.add_stage('Groovy Evaluator', type='processor')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> expression_evaluator >> groovy_evaluator >> wiretap.destination
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
