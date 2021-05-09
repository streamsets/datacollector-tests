# Copyright 2021 StreamSets Inc.
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
import pytest

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import sdc_min_version


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')

    return hook


@stub
def test_destroy_script(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_invokedynamic_compiler_option': False},
                                              {'enable_invokedynamic_compiler_option': True}])
def test_enable_invokedynamic_compiler_option(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_init_script(sdc_builder, sdc_executor):
    pass


@sdc_min_version('3.10.0')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD', 'error_expected': ''},
                                              {'on_record_error': 'STOP_PIPELINE', 'error_expected': 'SCRIPTING_06'},
                                              {'on_record_error': 'TO_ERROR', 'error_expected': 'SCRIPTING_04'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    """Test Groovy Evaluator processor. We test by setting up a template object in initialization script, then
    main processing script which access the template to create a Groovy object per record and to create a new record
    attribute. Finally in destroy script, we collate all the records for its new attribute and Groovy assert its value.
    The pipeline would look like:

        groovy_scripting_origin >> groovy_evaluator >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    error_code = ''
    error_message_expected = 'Groovy STF force error'
    script_origin = """
    entityName = ''
    offset = 0
    cur_batch = sdc.createBatch()
    record = sdc.createRecord('generated data')

    hasNext = true
    while (hasNext) {
        try {
            offset = offset + 1
            value = entityName + ':' + offset.toString()
            record.value = value
            cur_batch.add(record)
            if (cur_batch.size() >= sdc.batchSize) {
                cur_batch.process(entityName, offset.toString())
                cur_batch = sdc.createBatch()
                if (sdc.isStopped()) {
                    hasNext = false
                }
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

    groovy_origin = pipeline_builder.add_stage('Groovy Scripting')
    groovy_origin.set_attributes(record_type='NATIVE_OBJECTS',
                                 user_script=script_origin,
                                 batch_size=2,
                                 number_of_threads=1)

    groovy_evaluator = pipeline_builder.add_stage('Groovy Evaluator', type='processor')
    script_processor = '''
    records = sdc.records
    count = 1
    for (record in records) {
      try {
        records.each {
          if (count == 1) {
            sdc.error.write(record, 'Groovy STF force error')
          } else {
            sdc.output.write(record)
          }
          count = count + 1
        }
      } catch (Exception ex) {
        // Write a record to the error pipeline
        sdc.log.error(ex.toString(), ex)
        sdc.error.write(record, ex.toString())
      }
    }
    '''
    groovy_evaluator.set_attributes(record_processing_mode='BATCH',
                                    script=script_processor,
                                    init_script='',
                                    destroy_script='',
                                    on_record_error=stage_attributes['on_record_error'])

    wiretap = pipeline_builder.add_wiretap()

    groovy_origin >> groovy_evaluator >> wiretap.destination

    pipeline = pipeline_builder.build()
    pipeline.configuration['runnerIdleTIme'] = 2
    pipeline.configuration['shouldRetry'] = False

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(10)
        sdc_executor.stop_pipeline(pipeline)
    except Exception as e:
        error_msg = e.message
        error_code = error_msg
    finally:
        if stage_attributes['on_record_error'] == 'TO_ERROR':
            error_msg = wiretap.error_records[0].header['errorMessage']
            error_code = wiretap.error_records[0].header['errorCode']
        assert stage_attributes['error_expected'] in error_code
        if stage_attributes['on_record_error'] != 'DISCARD':
            assert error_message_expected in error_msg


@stub
def test_parameters_in_script(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_processing_mode': 'BATCH'}, {'record_processing_mode': 'RECORD'}])
def test_record_processing_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_type': 'NATIVE_OBJECTS'}, {'record_type': 'SDC_RECORDS'}])
def test_record_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_script(sdc_builder, sdc_executor):
    pass

