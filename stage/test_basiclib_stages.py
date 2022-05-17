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
import string
import tempfile

from streamsets.sdk import sdc_api
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


def test_stream_selector_processor(sdc_builder, sdc_executor):
    """Smoke test for the Stream Selector processor.

    A handful of records containing Tour de France contenders and their number of wins is passed
    to a Stream Selector with multi-winners going to one wiretap stage and not multi-winners going
    to another.

                                               >> to_error
    dev_raw_data_source >> record_deduplicator >> stream_selector >> wiretap (multi-winners)
                                                                  >> wiretap (not multi-winners)
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
                                       raw_data=json.dumps(tour_de_france_contenders),
                                       stop_after_first_batch=True)
    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    to_error = pipeline_builder.add_stage('To Error')
    stream_selector = pipeline_builder.add_stage('Stream Selector')
    wiretap_multi_winners = pipeline_builder.add_wiretap()
    wiretap_not_multi_winners = pipeline_builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> stream_selector >> wiretap_multi_winners.destination
    record_deduplicator >> to_error
    stream_selector >> wiretap_not_multi_winners.destination

    stream_selector.condition = [dict(outputLane=stream_selector.output_lanes[0],
                                      predicate='${record:value("/wins") > 1}'),
                                 dict(outputLane=stream_selector.output_lanes[1],
                                      predicate='default')]

    pipeline = pipeline_builder.build('test_stream_selector_processor')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    multi_winners_records = wiretap_multi_winners.output_records
    multi_winners_from_records = [{key: value
                                   for key, value in record.field.items()}
                                  for record in multi_winners_records]
    assert multi_winners == multi_winners_from_records

    not_multi_winners_records = wiretap_not_multi_winners.output_records
    not_multi_winners_from_records = [{field: value
                                       for field, value in record.field.items()}
                                      for record in not_multi_winners_records]
    assert not_multi_winners == not_multi_winners_from_records


# Log Parser tests


@sdc_min_version('3.9.0')
def test_log_parser_processor_multiple_grok_patterns(sdc_builder, sdc_executor):
    """
    Test that logs matching different grok patterns are correctly parsed when all corresponding grok patterns are
    present in the grok patterns list configuration variable of log_parser stage. Pipeline looks like:

    dev_raw_data_source >> log_parser >> wiretap
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
                                                                                   grok_pattern_definition="JUSTLOGLEVEL %{LOGLEVEL:log-level}",
                                                                                   grok_patterns=[
                                                                                       '',
                                                                                       '%{COMBINEDAPACHELOG}',
                                                                                       '%{JUSTLOGLEVEL}'
                                                                                   ])

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> log_parser_processor >> wiretap.destination

    log_parser_pipeline = pipeline_builder.build(
        title='Log Parser pipeline multiple grok patterns success')

    sdc_executor.add_pipeline(log_parser_pipeline)

    sdc_executor.start_pipeline(log_parser_pipeline).wait_for_finished()
    records = [record.field for record in wiretap.output_records]

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

    assert records[1]['parsed_log']['log-level'] == 'DEBUG'


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

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> log_parser_processor >> wiretap.destination

    log_parser_pipeline = pipeline_builder.build(
        title='Log Parser pipeline multiple grok patterns success')

    sdc_executor.add_pipeline(log_parser_pipeline)

    sdc_executor.start_pipeline(log_parser_pipeline).wait_for_finished()
    correct_records = [record.field for record in wiretap.output_records]

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

    bad_records = [record.field for record in wiretap.error_records]

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
    executor.reset_origin = reset_offset

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


@sdc_min_version('5.2.0')
def test_pipeline_finisher_react_to_events(sdc_builder, sdc_executor):
    """
    Tests the 'React to Events' option in the Pipeline Finisher stage.

    The test creates a pipeline 'Directory (>= Pipeline Finisher) >> Trash' to check the pipeline is finished once the
    Pipeline Finisher receives the event defined via the 'React to Events' option. In this case, the event defined is
    the 'no-more-data' event.
    """

    # Create a file
    file_path = tempfile.gettempdir()
    file_name = f'{get_random_string(string.ascii_letters, 10)}'
    _create_file(sdc_executor, 'Hello World!', file_path, file_name)

    # Create the pipeline Directory (>= Pipeline Finisher) >> Trash
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(
        data_format='TEXT',
        file_name_pattern=file_name,
        file_name_pattern_mode='GLOB',
        file_post_processing='DELETE',
        files_directory=file_path,
        batch_size_in_recs=100
    )

    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(
        react_to_events=True,
        event_type="no-more-data"
    )
    directory >= pipeline_finisher

    trash = builder.add_stage('Trash')
    directory >> trash

    pipeline = builder.build('Pipeline Finisher with React to Events pipeline')
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=30)
        assert sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'FINISHED'

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.2.0')
def test_pipeline_finisher_react_to_events_unexpected_event(sdc_builder, sdc_executor):
    """
    Tests the 'React to Events' option in the Pipeline Finisher stage.

    The test creates a pipeline 'Directory (>= Pipeline Finisher) >> Trash' to check the Pipeline Finisher doesn't end
    the pipeline if the 'React to Events' option is on and the record received is a different kind of event. In such
    case an error record should be created instead of finishing the pipeline.

    As Wiretap cannot be attached to the Pipeline Finisher, in order to check the error record created we have to set
    the 'On Record Error' policy to 'Stop Pipeline' and then check the pipeline has stopped because of the error
    handling policies and not due to the Pipeline Finisher.
    """

    # Create a file
    file_path = tempfile.gettempdir()
    file_name = f'{get_random_string(string.ascii_letters, 10)}'
    _create_file(sdc_executor, 'Hello World!', file_path, file_name)

    # Create the pipeline Directory (>= Pipeline Finisher) >> Trash
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(
        data_format='TEXT',
        file_name_pattern=file_name,
        file_name_pattern_mode='GLOB',
        file_post_processing='DELETE',
        files_directory=file_path,
        batch_size_in_recs=100
    )

    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(
        react_to_events=True,
        event_type="file-created",
        on_record_error='STOP_PIPELINE'
    )
    directory >= pipeline_finisher

    trash = builder.add_stage('Trash')
    directory >> trash

    pipeline = builder.build('Pipeline Finisher with React to Events pipeline')
    sdc_executor.add_pipeline(pipeline)

    try:
        # As we can't add Wiretap after the Pipeline Finisher, we can only check the error produced with the Pipeline
        # Finisher's On Error Record policy, which will arise a RunError as soon as a different event is received
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert False, 'The test should not reach here, a RunError should have been thrown'
    except sdc_api.RunError as error:
        error_message = "PIPELINE_FINISHER_002 - Unsatisfied stage condition: The event is not of type 'file-created'"
        assert error_message in error.message
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.2.0')
def test_pipeline_finisher_react_to_events_not_an_event(sdc_builder, sdc_executor):
    """
    Tests the 'React to Events' option in the Pipeline Finisher stage.

    The test creates a pipeline 'Dev Raw Data Source >> Pipeline Finisher' to check the Pipeline Finisher doesn't end
    the pipeline if the 'React to Events' option is on and the record received is not an event. In such case an error
    record should be created instead of finishing the pipeline.

    As Wiretap cannot be attached to the Pipeline Finisher, in order to check the error record created we have to set
    the 'On Record Error' policy to 'Stop Pipeline' and then check the pipeline has stopped because of the error
    handling policies and not due to the Pipeline Finisher.
    """

    # Create the pipeline Dev Raw Data Source >> Pipeline Finisher
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data='Hello World!',
        stop_after_first_batch=True
    )

    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(
        react_to_events=True,
        event_type="no-more-data",
        on_record_error='STOP_PIPELINE'
    )

    dev_raw_data_source >> pipeline_finisher

    pipeline = builder.build('Pipeline Finisher with React to Events pipeline')
    sdc_executor.add_pipeline(pipeline)

    try:
        # As we can't add Wiretap after the Pipeline Finisher, we can only check the error produced with the Pipeline
        # Finisher's On Error Record policy, which will arise a RunError once a record that is not an event is read
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert False, 'The test should not reach here, a RunError should have been thrown'
    except sdc_api.RunError as error:
        error_message = "PIPELINE_FINISHER_001 - Unsatisfied stage condition: The record is not an event"
        assert error_message in error.message
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.2.0')
def test_pipeline_finisher_react_to_events_missing_event(sdc_builder, sdc_executor):
    """
    Tests the 'React to Events' option in the Pipeline Finisher stage.

    The test creates a pipeline 'Dev Raw Data Source >> Pipeline Finisher' to check a Validation Error arises if the
    'React to Events' option is on but no event is defined.
    """

    # Create the pipeline Dev Raw Data Source >> Pipeline Finisher
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data='Hello World!',
        stop_after_first_batch=True
    )

    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(
        react_to_events=True,
        event_type="",
        on_record_error='STOP_PIPELINE'
    )

    dev_raw_data_source >> pipeline_finisher

    pipeline = builder.build('Pipeline Finisher with React to Events pipeline')
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.validate_pipeline(pipeline)
        assert False, 'The test should not reach here, a validation error should have been thrown'
    except Exception as error:
        error_message = 'VALIDATION_0007 - Configuration value is required'
        assert error_message in error.issues


# SDC-11555: Provide ability to use direct SDC record in scripting processors
@sdc_min_version('3.9.0')
def test_javascript_sdc_record(sdc_builder, sdc_executor):
    """Iterate over SDC record directly rather then JSR-223 wrapper."""
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{"old": "old-value"}', stop_after_first_batch=True)
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

    wiretap = builder.add_wiretap()

    origin >> javascript >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    records = wiretap.output_records

    assert len(records) == 1
    assert 'new-value' == records[0].field['new']
    assert 'attr-value' == records[0].get_field_data('/old').attributes['attr']


def test_javascript_event_creation(sdc_builder, sdc_executor):
    """Ensure that the process is able to create events."""
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{"old": "old-value"}', stop_after_first_batch=True)
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

    identity = builder.add_stage('Dev Identity')
    trash = builder.add_stage('Trash')

    wiretap = builder.add_wiretap()

    origin >> javascript >> trash
    javascript >= identity
    identity >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/value') == 'secret'


def test_expression_evaluator(sdc_builder, sdc_executor):
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = '{"a" : "b"}'
    source.stop_after_first_batch = True

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        "fieldToSet" : "/new_field",
        "expression" : "Secret 1"
    }]
    expression.header_attribute_expressions = [{
        "attributeToSet" : "new header",
        "headerAttributeExpression" : "Secret 2"
    }]
    expression.field_attribute_expressions = [{
        "fieldToSet" : "/a",
        "attributeToSet" : "new field header",
        "fieldAttributeExpression" : "Secret 3"
    }]

    wiretap = builder.add_wiretap()

    source >> expression >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/new_field') == 'Secret 1'
    assert wiretap.output_records[0].header['values']['new header'] == 'Secret 2'
    assert wiretap.output_records[0].get_field_data('/a').attributes['new field header'] == 'Secret 3'


def test_expression_evaluator_self_referencing_expression(sdc_builder, sdc_executor):
    """Expression Evaluator supports records to self-reference. We need to make sure that the self-refercing works
    properly and doesn't run into any issues like StackOverflowException (which could happen with some optimizations
    like the one done in SDC-14645)."""
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = '{"a" : "b"}'
    source.stop_after_first_batch = True

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        "fieldToSet": "/new_field",
        # We're using record self-referencing expression
        "expression": "${record:value('/')}"
    }]

    wiretap = builder.add_wiretap()

    source >> expression >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/new_field/a') == 'b'


@sdc_min_version('3.16.0')
def test_deduplicator_field_to_compare(sdc_builder, sdc_executor):
    """When field to compare in Record Deduplicator stage doesn't exists, it use On Record Error to manage
    the DEDUP error. The record error has to be "DEDUP_04: Field Path does not exist in the record".

    dev_raw_data_source >> record_deduplicator  >> wiretap
                                                >> wiretap
    """
    raw_data = "Hello"
    field_to_compare = ["/ff", "/text"]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    record_deduplicator.set_attributes(on_record_error='TO_ERROR',
                                       compare="SPECIFIED_FIELDS",
                                       fields_to_compare=field_to_compare)
    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> wiretap_1.destination
    record_deduplicator >> wiretap_2.destination

    pipeline = pipeline_builder.build('test_deduplicator_field_to_compare')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert 1 == len(wiretap_2.error_records)
    assert 'DEDUP_04' == wiretap_2.error_records[0].header['errorCode']


def test_log_parser_cef_format(sdc_builder, sdc_executor):
    """This is a test for SDC-15376. It creates a pipeline with three stages.
    A dev_raw that produces a string with CEF format. The log record correctness is asserted.
    Pipeline looks like:
    DevRawDataSource >> log_parser_processor >> wiretap
    """

    record_text = "CEF:0|Tone Computacion|M Axes|3.19|port_scan|Port Scan|6|externalId=49062 cat=RECONNAISSANCE " \
        "dvc=162.1.2.3 dvchost=162.1.2.3 shost=IP-162.2.2.3 src=162.2.2.3 flexNumber1Label=threat flexNumber1=60 " \
        "flexNumber2Label=certainty flexNumber2=80 cs4Label=Tone Event URL cs4=https://162.1.2.3/detections/" \
        "49062 cs5Label=triaged cs5=False dst=162.3.2.3 dhost= proto=tcp dpt=80 out=None in=None " \
        "start=2573599048000 end=2584316799000"

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=record_text,
                                       stop_after_first_batch=True)
    log_parser_processor = builder.add_stage('Log Parser')
    log_parser_processor.set_attributes(field_to_parse='/text', new_parsed_field='/cef', log_format="CEF")
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> log_parser_processor >> wiretap.destination
    cef_parser_pipeline = builder.build().configure_for_environment()
    sdc_executor.add_pipeline(cef_parser_pipeline)

    try:
        # start pipeline and process one record
        sdc_executor.start_pipeline(cef_parser_pipeline).wait_for_finished()

        assert wiretap.output_records[0].field['cef']['product'].value == 'M Axes'
        assert wiretap.output_records[0].field['cef']['extensions'][
                   'triaged'].value == 'False'
        assert wiretap.output_records[0].field['cef']['extensions'][
                   'dst'].value == '162.3.2.3'
        assert wiretap.output_records[0].field['cef']['extensions'][
                   'src'].value == '162.2.2.3'
        assert wiretap.output_records[0].field['cef']['extensions']['dhost'].value == ''
        assert wiretap.output_records[0].field['cef']['extensions']['proto'].value == 'tcp'
        assert wiretap.output_records[0].field['cef']['extensions']['dpt'].value == '80'
        assert wiretap.output_records[0].field['cef']['extensions']['out'].value == 'None'
        assert wiretap.output_records[0].field['cef']['extensions'][
                   'start'].value == '2573599048000'
        assert wiretap.output_records[0].field['cef']['extensions'][
                   'end'].value == '2584316799000'

    finally:
        if cef_parser_pipeline and sdc_executor.get_pipeline_status(cef_parser_pipeline).response.json().get(
                'status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(cef_parser_pipeline)


def _create_file(sdc_executor, file_content, file_path, file_name):
    builder = sdc_executor.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data=file_content,
        stop_after_first_batch=True
    )

    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(
        data_format='TEXT',
        directory_template=file_path,
        files_prefix=file_name,
        files_suffix=''
    )

    dev_raw_data_source >> local_fs
    file_pipeline = builder.build(f'Generate files pipeline {file_path}')
    sdc_executor.add_pipeline(file_pipeline)

    sdc_executor.start_pipeline(file_pipeline).wait_for_finished()
    sdc_executor.remove_pipeline(file_pipeline)
