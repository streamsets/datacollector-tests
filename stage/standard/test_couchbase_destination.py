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

import json
import logging
import pytest
import string

from avro.datafile import DataFileReader
from avro.io import DatumReader
from couchbase.transcoder import RawJSONTranscoder, RawStringTranscoder, RawBinaryTranscoder, Transcoder
from io import BytesIO

from streamsets.testframework.markers import couchbase, sdc_min_version
from streamsets.testframework.utils import get_random_string, sdc_value_reader

logger = logging.getLogger(__name__)

pytestmark = [couchbase, sdc_min_version('3.4.0')]

SUPPORTED_LIBS = ['streamsets-datacollector-couchbase_2-lib', 'streamsets-datacollector-couchbase_3-lib']
STAGE_NAME = 'com_streamsets_pipeline_stage_destination_couchbase_CouchbaseDTarget'

DEFAULT_SCOPE = '_default'
DEFAULT_COLLECTION = '_default'

# Reference: https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/datatypes.html
DATA_TYPES = [
    ('true', 'BOOLEAN', True),
    ('a', 'CHAR', 'a'),
    ('65', 'BYTE', 'QQ=='),
    (120, 'SHORT', 120),
    (120, 'INTEGER', 120),
    (120, 'LONG', 120),
    (20.1, 'FLOAT', 20.1),
    (20.1, 'DOUBLE', 20.1),
    (20.1, 'DECIMAL', 20.1),
    (-20.1, 'DECIMAL', -20.1),
    ('2020-01-01 10:00:00', 'DATE', 1577872800000),
    ('2020-01-01 10:00:00', 'TIME', 1577872800000),
    ('2020-01-01 10:00:00', 'DATETIME', 1577872800000),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', '2020-01-01T10:00:00Z'),
    ('string', 'STRING', 'string'),
    ("Hello", 'BYTE_ARRAY', 'SGVsbG8='),
]

# Reference: https://docs.couchbase.com/server/current/manage/manage-buckets/create-bucket.html
COUCHBASE_BUCKET_NAMES = [
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 100)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('uppercase', lambda: get_random_string(string.ascii_uppercase)),
    ('hyphen', lambda: get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('underscore', lambda: get_random_string(string.ascii_lowercase) + '_' + get_random_string(string.ascii_lowercase)),
    ('percent', lambda: get_random_string(string.ascii_lowercase) + '%' + get_random_string(string.ascii_lowercase)),
    ('digits', lambda: get_random_string(string.digits))
]

PROTOBUF_FILE_PATH = 'resources/protobuf/addressbook.desc'


@pytest.fixture(autouse=True, scope='module')
def init(couchbase):
    for lib in couchbase.sdc_stage_libs:
        if lib in SUPPORTED_LIBS:
            couchbase.pre_create_buckets()
            return
    pytest.skip(f'Couchbase Destination test requires using libraries in {SUPPORTED_LIBS}')


@pytest.mark.parametrize('input,converter_type,expected', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, couchbase, input, converter_type, expected):
    document_key_field = 'mydocname'
    raw_dict = {"value": input, document_key_field: 'mydocid'}
    raw_data = json.dumps(raw_dict)
    try:
        bucket_name = couchbase.get_bucket()
        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        converter = builder.add_stage('Field Type Converter')
        converter.conversion_method = 'BY_FIELD'
        converter.field_type_converter_configs = [{
            'fields': ['/value'],
            'targetType': converter_type,
            'dataLocale': 'en,US',
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
            'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
            'scale': 2
        }]

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name,
                                   document_key="${record:value('/" + document_key_field + "')}")

        source >> converter >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        pipeline.configuration["shouldRetry"] = False
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        doc_value = couchbase.get_value(raw_dict[document_key_field], bucket_name)

        assert len(doc_value) == len(raw_dict)
        assert doc_value['value'] == expected
    finally:
        couchbase.cleanup_buckets()


@pytest.mark.parametrize('test_name, bucket_generator',
                         COUCHBASE_BUCKET_NAMES,
                         ids=[i[0] for i in COUCHBASE_BUCKET_NAMES])
def test_object_names_bucket(sdc_builder, sdc_executor, couchbase, test_name, bucket_generator):
    """Test using different kinds of bucket names, asserting whether Couchbase receives the data"""
    bucket_name = bucket_generator()
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)

    try:
        couchbase.create_bucket(bucket_name)

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name,
                                   document_key="${record:value('/" + document_key_field + "')}")

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        doc_value = couchbase.get_value(raw_dict[document_key_field], bucket_name)
        assert doc_value == raw_dict
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('batch_size', [1, 10])
def test_multiple_batches(sdc_builder, sdc_executor, couchbase, batch_size):
    batches = 3

    try:
        bucket_name = couchbase.get_bucket()

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Data Generator')
        source.batch_size = batch_size
        source.fields_to_generate = [{
            "type": "LONG_SEQUENCE",
            "field": "seq"
        }]

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name, document_key='${record:value("/seq")}')

        wiretap = builder.add_wiretap()

        source >> destination
        source >= wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        assert num_records == len(wiretap.output_records)

        for i in range(num_records):
            assert couchbase.get_value(str(i), bucket_name) == wiretap.output_records[i].field

    finally:
        couchbase.cleanup_buckets()


def test_dataflow_events(sdc_builder, sdc_executor, couchbase):
    pytest.skip('Couchbase Destination does not produce events.')


def test_data_format_avro(sdc_builder, sdc_executor, couchbase):
    document_key = 'id'

    DATA = {'name': 'boss', 'age': 60, 'emails': ['boss@company.com', 'boss2@company.com'], 'boss': None}
    SCHEMA = {'namespace': 'example.avro',
              'type': 'record',
              'name': 'Employee',
              'fields': [{'name': 'name', 'type': 'string'},
                         {'name': 'age', 'type': 'int'},
                         {'name': 'emails', 'type': {'type': 'array', 'items': 'string'}},
                         {'name': 'boss', 'type': ['Employee', 'null']}]}

    try:
        bucket_name = couchbase.get_bucket()
        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                         raw_data=json.dumps(DATA),
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name, document_key=document_key,
                                   data_format='AVRO', avro_schema=json.dumps(SCHEMA), avro_schema_location='INLINE')

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        doc_value = couchbase.get_value(document_key, bucket_name, transcoder=RawBinaryTranscoder())

        # decode the bytes object returned by Couchbase
        file = BytesIO(doc_value)
        reader = DataFileReader(file, DatumReader())
        records = [record for record in reader]
        assert len(records) == 1, 'Number of records stored should equal number of records that entered the pipeline'
        assert records[0] == DATA
        reader.close()
    finally:
        couchbase.cleanup_buckets()


def test_data_format_binary(sdc_builder, sdc_executor, couchbase):
    document_key = 'id'
    batch_size = 1

    try:
        bucket_name = couchbase.get_bucket()

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Data Generator').set_attributes(batch_size=batch_size,
                                                                        fields_to_generate=[{
                                                                            "type": "BYTE_ARRAY",
                                                                            "field": "data"
                                                                        }])

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name, document_key=document_key,
                                   data_format='BINARY', binary_field_path="/data")

        wiretap = builder.add_wiretap()

        source >> destination
        source >= wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batch_size)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        doc_value = couchbase.get_value(document_key, bucket_name, transcoder=RawBinaryTranscoder())
        assert num_records == len(wiretap.output_records)
        assert doc_value == wiretap.output_records[0].field['data']
    finally:
        couchbase.cleanup_buckets()


def test_data_format_delimited(sdc_builder, sdc_executor, couchbase):
    document_key = 'id'
    raw_data = 'Alex,Xavi,Tucu,Martin'
    expected = 'Alex,Xavi,Tucu,Martin\r'

    try:
        bucket_name = couchbase.get_bucket()
        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='DELIMITED',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name, document_key=document_key,
                                   data_format='DELIMITED')

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        assert num_records == 1, 'Number of records stored should equal the number of records that entered the pipeline'

        doc_value = couchbase.get_value(document_key, bucket_name, transcoder=RawBinaryTranscoder())
        # Decode the bytes object returned by Couchbase and remove the empty final line
        contents = doc_value.decode('ascii').replace('\n', '')
        assert contents == expected
    finally:
        couchbase.cleanup_buckets()


def test_data_format_json(sdc_builder, sdc_executor, couchbase):
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)
    try:
        bucket_name = couchbase.get_bucket()

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name,
                                   document_key="${record:value('/" + document_key_field + "')}",
                                   data_format='JSON')

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        doc_value = couchbase.get_value(raw_dict[document_key_field], bucket_name)
        assert doc_value == raw_dict
    finally:
        couchbase.cleanup_buckets()


def test_data_format_protobuf(sdc_builder, sdc_executor, couchbase):
    document_key = 'id'
    raw_data = '{"first_name": "Martin","last_name": "Balzamo"}'
    expected = '\x11\x06Martin\x12\x07Balzamo'

    try:
        bucket_name = couchbase.get_bucket()
        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name, document_key=document_key,
                                   data_format='PROTOBUF', message_type='Contact',
                                   protobuf_descriptor_file=PROTOBUF_FILE_PATH)

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        assert num_records == 1, 'Number of records stored should equal the number of records that entered the pipeline'

        doc_value = couchbase.get_value(document_key, bucket_name, transcoder=RawBinaryTranscoder())
        # Decode the bytes object returned by Couchbase and remove any record separators (newline characters)
        contents = doc_value.decode('ascii').replace('\n', '')
        assert contents == expected
    finally:
        couchbase.cleanup_buckets()


def test_data_format_sdc_record(sdc_builder, sdc_executor, couchbase):
    document_key_field = 'field1'
    json_data = [{"field1": "abc", "field2": "def", "field3": "ghi"},
                 {"field1": "jkl", "field2": "mno", "field3": "pqr"}]
    raw_data = ''.join(json.dumps(record) for record in json_data)

    try:
        bucket_name = couchbase.get_bucket()

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name,
                                   document_key="${record:value('/" + document_key_field + "')}",
                                   data_format='SDC_JSON')

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        assert num_records == len(json_data)

        for i in range(len(json_data)):
            doc_value = couchbase.get_value(json_data[i][document_key_field], bucket_name, transcoder=RawBinaryTranscoder())
            # Decode the bytes object and disregard the first character (0xa1)
            contents = doc_value.decode('latin1')[1:]
            # Decode the SDC Record JSON into a dictionary containing its value
            dictionary = json.loads(contents)
            value = sdc_value_reader(dictionary['value'])
            assert value == json_data[i]
    finally:
        couchbase.cleanup_buckets()


def test_data_format_text(sdc_builder, sdc_executor, couchbase):
    document_key = 'id'
    raw_data = get_random_string(string.ascii_letters, length=100)

    try:
        bucket_name = couchbase.get_bucket()

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                         raw_data=raw_data,
                                                                         stop_after_first_batch=True)

        destination = builder.add_stage(name=STAGE_NAME)
        destination.set_attributes(authentication_mode='USER', bucket=bucket_name, document_key=document_key,
                                   data_format='TEXT', text_field_path="/text", record_separator="\n")

        source >> destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        assert num_records == 1, 'Number of records stored should equal the number of records that entered the pipeline'

        doc_value = couchbase.get_value(document_key, bucket_name, transcoder=RawBinaryTranscoder())
        # Decode the bytes object returned by Couchbase and remove any record separators (newline characters)
        contents = doc_value.decode('ascii').replace('\n', '')
        assert contents == raw_data
    finally:
        couchbase.cleanup_buckets()


def test_push_pull(sdc_builder, sdc_executor, couchbase):
    pytest.skip("Already tested test since Dev Data Generator (push) is part of test_multiple_batches,"
                " and Dev Raw Data Source (pull) is part of test_data_types.")
