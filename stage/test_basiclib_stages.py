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
import string

import pytest

from streamsets.sdk.models import Configuration
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.sdc_properties['stage.conf_com.streamsets.pipeline.stage.executor'
                                      '.shell.impersonation_mode'] = 'current_user'

    return hook


@pytest.fixture(scope='module')
def pipeline_shell_generator(sdc_executor):
    builder = sdc_executor.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{}'

    shell_executor = builder.add_stage('Shell')
    shell_executor.environment_variables = Configuration(property_key='key', file='${FILE}')
    shell_executor.script = 'echo `whoami` > $file'

    dev_raw_data_source >> shell_executor

    executor_pipeline = builder.build()
    executor_pipeline.add_parameters(FILE='/')
    sdc_executor.add_pipeline(executor_pipeline)

    yield executor_pipeline


@pytest.fixture(scope='module')
def pipeline_shell_read(sdc_executor):
    builder = sdc_executor.get_pipeline_builder()

    file_source = builder.add_stage('File Tail')
    file_source.data_format = 'TEXT'
    file_source.file_to_tail = [
        dict(fileRollMode='REVERSE_COUNTER', patternForToken='.*', fileFullPath='${FILE}')
    ]

    trash1 = builder.add_stage('Trash')
    trash2 = builder.add_stage('Trash')

    file_source >> trash1
    file_source >> trash2

    read_pipeline = builder.build()
    read_pipeline.add_parameters(FILE='/')
    sdc_executor.add_pipeline(read_pipeline)

    yield read_pipeline


def test_shell_executor_impersonation(sdc_executor, pipeline_shell_generator, pipeline_shell_read):
    """Test proper impersonation on the Shell executor side. This is a dual pipeline test to test the executor
    side effect.
    Test fails till TEST-128 is addressed.
    """

    # Use this file to exchange data between the executor and our test
    runtime_parameters = {'FILE': "/tmp/{}".format(get_random_string(string.ascii_letters, 30))}

    # Run the pipeline with executor exactly once
    sdc_executor.start_pipeline(pipeline_shell_generator,
                                runtime_parameters=runtime_parameters).wait_for_pipeline_batch_count(1)
    sdc_executor.stop_pipeline(pipeline_shell_generator)

    # And retrieve its output
    snapshot = sdc_executor.capture_snapshot(pipeline=pipeline_shell_read, runtime_parameters=runtime_parameters,
                                             start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline_shell_read)

    records = snapshot[pipeline_shell_read.origin_stage].output_lanes[pipeline_shell_read.origin_stage.output_lanes[0]]
    assert len(records) == 1
    # Blocked by TEST-128, should be different user
    assert records[0].field['text'].value == 'sdc'


def test_stream_selector_processor(sdc_builder, sdc_executor):
    """Smoke test for the Stream Selector processor.

    A handful of records containing Tour de France contenders and their number of wins is passed
    to a Stream Selector with multi-winners going to one Trash stage and not multi-winners going
    to another.

                                               >> to_error
    dev_raw_data_source >> record_deduplicator >> stream_selector >> trash (multi-winners)
                                                                  >> trash (not multi-winners)
    """
    multi_winners = [dict(name='Chris Froome', wins='3'),
                     dict(name='Greg LeMond', wins='3')]
    not_multi_winners = [dict(name='Vincenzo Nibali', wins='1'),
                         dict(name='Nairo Quintana', wins='0')]

    tour_de_france_contenders = multi_winners + not_multi_winners

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       json_content='ARRAY_OBJECTS',
                                       raw_data=json.dumps(tour_de_france_contenders))
    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    to_error = pipeline_builder.add_stage('To Error')
    stream_selector = pipeline_builder.add_stage('Stream Selector')
    trash_multi_winners = pipeline_builder.add_stage('Trash')
    trash_not_multi_winners = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> stream_selector >> trash_multi_winners
    record_deduplicator >> to_error
    stream_selector >> trash_not_multi_winners

    stream_selector.condition = [dict(outputLane=stream_selector.output_lanes[0],
                                      predicate='${record:value("/wins") > 1}'),
                                 dict(outputLane=stream_selector.output_lanes[1],
                                      predicate='default')]

    pipeline = pipeline_builder.build('test_stream_selector_processor')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    multi_winners_records = snapshot[stream_selector].output_lanes[stream_selector.output_lanes[0]]
    multi_winners_from_snapshot = [{key: value
                                    for key, value in record.field.items()}
                                   for record in multi_winners_records]
    assert multi_winners == multi_winners_from_snapshot

    not_multi_winners_records = snapshot[stream_selector].output_lanes[stream_selector.output_lanes[1]]
    not_multi_winners_from_snapshot = [{field: value
                                        for field, value in record.field.items()}
                                       for record in not_multi_winners_records]
    assert not_multi_winners == not_multi_winners_from_snapshot


# Log Parser tests


@sdc_min_version('3.9.0')
def test_log_parser_processor_multiple_grok_patterns(sdc_builder, sdc_executor):
    """
    Test that logs matching different grok patterns are correctly parsed when all corresponding grok patterns are
    present in the grok patterns list configuration variable of log_parser stage. Pipeline looks like:

    dev_raw_data_source >> log_parser >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    json_fields_map = [
        {
            'log': '111.222.333.123 HOME - [01/Feb/1998:01:08:46 -0800] "GET /dummy/dummy.htm HTTP/1.0" 200 28083 '
                   '"http://www.streamsets.com/dummy/dummy_intro.htm" "Mozilla/4.01 (Macintosh; I; PPC)"'
        },
        {
            'log': 'DEBUG'
        }
    ]
    json_str = '\n'.join(json.dumps(record) for record in json_fields_map)

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=json_str,
                                                                                           stop_after_first_batch=True)

    log_parser_processor = pipeline_builder.add_stage('Log Parser').set_attributes(field_to_parse='/log',
                                                                                   new_parsed_field='/parsed_log',
                                                                                   log_format="GROK",
                                                                                   grok_patterns=[
                                                                                       '',
                                                                                       '%{COMBINEDAPACHELOG}',
                                                                                       '%{LOGLEVEL}'
                                                                                   ])

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> log_parser_processor >> trash

    log_parser_pipeline = pipeline_builder.build(
        title='Log Parser pipeline multiple grok patterns success')

    sdc_executor.add_pipeline(log_parser_pipeline)

    snapshot_cmd = sdc_executor.capture_snapshot(log_parser_pipeline, start_pipeline=True, wait=False)
    snapshot = snapshot_cmd.wait_for_finished().snapshot
    records = [record.field for record in snapshot[log_parser_processor.instance_name].output]

    assert 2 == len(records)

    assert records[0]['parsed_log']['request'] == '/dummy/dummy.htm'
    assert records[0]['parsed_log']['agent'] == '"Mozilla/4.01 (Macintosh; I; PPC)"'
    assert records[0]['parsed_log']['auth'] == '-'
    assert records[0]['parsed_log']['ident'] == 'HOME'
    assert records[0]['parsed_log']['verb'] == 'GET'
    assert records[0]['parsed_log']['referrer'] == '"http://www.streamsets.com/dummy/dummy_intro.htm"'
    assert records[0]['parsed_log']['response'] == '200'
    assert records[0]['parsed_log']['bytes'] == '28083'
    assert records[0]['parsed_log']['clientip'] == '111.222.333.123'
    assert records[0]['parsed_log']['httpversion'] == '1.0'
    assert records[0]['parsed_log']['timestamp'] == '01/Feb/1998:01:08:46 -0800'

    assert records[1]['parsed_log']['parsedLine'] == 'DEBUG'


@sdc_min_version('3.9.0')
def test_log_parser_processor_multiple_grok_patterns_not_all_present(sdc_builder, sdc_executor):
    """
    Test that logs matching different grok patterns are correctly parsed when corresponding grok patterns are
    present in the grok patterns list configuration variable of log_parser stage and logs without a pattern that
    matches them are sent to error. Pipeline looks like:

    dev_raw_data_source >> log_parser >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    json_fields_map = [
        {
            'log': '111.222.333.123 HOME - [01/Feb/1998:01:08:46 -0800] "GET /dummy/dummy.htm HTTP/1.0" 200 28083 '
                   '"http://www.streamsets.com/dummy/dummy_intro.htm" "Mozilla/4.01 (Macintosh; I; PPC)"'
        },
        {
            'log': 'DEBUG'
        }
    ]
    json_str = '\n'.join(json.dumps(record) for record in json_fields_map)

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=json_str,
                                                                                           stop_after_first_batch=True)

    log_parser_processor = pipeline_builder.add_stage('Log Parser').set_attributes(field_to_parse='/log',
                                                                                   new_parsed_field='/parsed_log',
                                                                                   log_format="GROK",
                                                                                   grok_patterns=[
                                                                                       '',
                                                                                       '%{COMBINEDAPACHELOG}'
                                                                                   ])

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> log_parser_processor >> trash

    log_parser_pipeline = pipeline_builder.build(
        title='Log Parser pipeline multiple grok patterns success')

    sdc_executor.add_pipeline(log_parser_pipeline)

    snapshot_cmd = sdc_executor.capture_snapshot(log_parser_pipeline, start_pipeline=True, wait=False)
    snapshot = snapshot_cmd.wait_for_finished().snapshot
    correct_records = [record.field for record in snapshot[log_parser_processor.instance_name].output]

    assert 1 == len(correct_records)

    assert correct_records[0]['parsed_log']['request'] == '/dummy/dummy.htm'
    assert correct_records[0]['parsed_log']['agent'] == '"Mozilla/4.01 (Macintosh; I; PPC)"'
    assert correct_records[0]['parsed_log']['auth'] == '-'
    assert correct_records[0]['parsed_log']['ident'] == 'HOME'
    assert correct_records[0]['parsed_log']['verb'] == 'GET'
    assert correct_records[0]['parsed_log']['referrer'] == '"http://www.streamsets.com/dummy/dummy_intro.htm"'
    assert correct_records[0]['parsed_log']['response'] == '200'
    assert correct_records[0]['parsed_log']['bytes'] == '28083'
    assert correct_records[0]['parsed_log']['clientip'] == '111.222.333.123'
    assert correct_records[0]['parsed_log']['httpversion'] == '1.0'
    assert correct_records[0]['parsed_log']['timestamp'] == '01/Feb/1998:01:08:46 -0800'

    bad_records = [record.field for record in snapshot[log_parser_processor.instance_name].error_records]

    assert 1 == len(bad_records)

    assert 'DEBUG' == bad_records[0]['log']


@sdc_min_version('3.9.0')
@pytest.mark.parametrize('reset_offset', [True, False])
def test_pipeline_finisher(reset_offset, sdc_builder, sdc_executor):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.raw_data = '{}'

    executor = builder.add_stage('Pipeline Finisher Executor')
    executor.reset_offset = reset_offset

    origin >> executor
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # The pipeline should read/write exactly one record
    history = sdc_executor.get_pipeline_history(pipeline)
    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
    assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1

    # Let's validate offset
    offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
    assert offset is not None
    assert offset['offsets'] is not None

    if reset_offset:
        assert len(offset['offsets']) == 0
    else:
        assert len(offset['offsets']) == 1


# SDC-11555: Provide ability to use direct SDC record in scripting processors
@sdc_min_version('3.9.0')
def test_javascript_sdc_record(sdc_builder, sdc_executor):
    """Iterate over SDC record directly rather then JSR-223 wrapper."""
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{"old": "old-value"}')
    origin.stop_after_first_batch = True

    javascript = builder.add_stage('JavaScript Evaluator')
    javascript.record_type = 'SDC_RECORDS'
    javascript.init_script = ''
    javascript.destroy_script = ''
    javascript.script =  """
          var Field = Java.type('com.streamsets.pipeline.api.Field');
          for (var i = 0; i < records.length; i++) {
            records[i].sdcRecord.set('/new', Field.create(Field.Type.STRING, 'new-value'));
            records[i].sdcRecord.get('/old').setAttribute('attr', 'attr-value');
            output.write(records[i]);
          }
        """

    trash = builder.add_stage('Trash')

    origin >> javascript >> trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    records = snapshot[javascript].output

    assert len(records) == 1
    assert 'new-value' == records[0].field['new']
    assert 'attr-value' == records[0].get_field_data('/old').attributes['attr']


def test_javascript_event_creation(sdc_builder, sdc_executor):
    """Ensure that the process is able to create events."""
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{"old": "old-value"}')
    origin.stop_after_first_batch = True

    javascript = builder.add_stage('JavaScript Evaluator')
    javascript.init_script = ''
    javascript.destroy_script = ''
    javascript.script =  """
event = sdcFunctions.createEvent("event", 1)
event.value = sdcFunctions.createMap(true)
event.value['value'] = "secret"
sdcFunctions.toEvent(event)
"""

    # TLKT-248: Add ability to directly read events from snapshots
    identity = builder.add_stage('Dev Identity')
    event_trash = builder.add_stage('Trash')

    trash = builder.add_stage('Trash')

    origin >> javascript >> trash
    javascript >= identity
    identity >> event_trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    assert len(snapshot[identity].output) == 1
    assert snapshot[identity].output[0].get_field_data('/value') == 'secret'