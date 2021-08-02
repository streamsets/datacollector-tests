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

from streamsets.sdk.utils import Version
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import couchbase
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

DATA_TYPES = [
    ('true', 'BOOLEAN', True),
    ('a', 'CHAR', 'a'),
    # ('a', 'BYTE', None), # Not supported today
    (120, 'SHORT', 120),
    (120, 'INTEGER', 120),
    (120, 'LONG', 120),
    (20.1, 'FLOAT', 20.1),
    (20.1, 'DOUBLE', 20.1),
    (20.1, 'DECIMAL', 20.1),
    ('2020-01-01 10:00:00', 'DATE', 1577872800000),
    ('2020-01-01 10:00:00', 'TIME', 1577872800000),
    ('2020-01-01 10:00:00', 'DATETIME', 1577872800000),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', '2020-01-01T10:00:00Z'),
    ('string', 'STRING', 'string'),
    # ('string', 'BYTE_ARRAY', 'string'), # Not supported today
]

COUCHBASE_BUCKET_NAMES = [
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 100)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('uppercase', lambda: get_random_string(string.ascii_uppercase)),
    ('hyphen', lambda: get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('period', lambda: get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('underscore', lambda: get_random_string(string.ascii_lowercase) + '_' + get_random_string(string.ascii_lowercase)),
    ('percent', lambda: get_random_string(string.ascii_lowercase) + '%' + get_random_string(string.ascii_lowercase)),
    ('digits', lambda: get_random_string(string.digits))
]


@couchbase
@pytest.mark.parametrize('input,converter_type,expected', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, couchbase, input, converter_type, expected):
    couchbase_host = f'{couchbase.hostname}:{couchbase.port}'
    bucket_name = get_random_string(string.ascii_letters, 10)

    document_key_field = 'mydocname'
    raw_dict = {"value": input, document_key_field: 'mydocid'}
    raw_data = json.dumps(raw_dict)

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

    destination = builder.add_stage('Couchbase', type='destination')
    if Version(sdc_builder.version) < Version('3.9.0'):
        destination.set_attributes(database_version='VERSION5', unique_document_key_field=document_key_field,
                                   bucket=bucket_name, couchbase_user_name=couchbase.username,
                                   couchbase_user_password=couchbase.password, url=couchbase_host)
    else:
        destination.set_attributes(authentication_mode='USER',
                                   document_key="${record:value('/" + document_key_field + "')}",
                                   bucket=bucket_name, user_name=couchbase.username,
                                   password=couchbase.password, node_list=couchbase_host)

    source >> converter >> destination

    pipeline = builder.build().configure_for_environment(couchbase)
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.admin.bucket_create(name=bucket_name, bucket_type='couchbase', ram_quota=256)
        couchbase.wait_for_healthy_bucket(bucket_name)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        bucket = couchbase.cluster.open_bucket(bucket_name)
        doc_value = bucket.get(raw_dict[document_key_field]).value

        assert len(doc_value) == len(raw_dict)
        assert doc_value['value'] == expected
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.admin.bucket_delete(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@couchbase
@pytest.mark.parametrize('test_name, bucket_generator',
                         COUCHBASE_BUCKET_NAMES,
                         ids=[i[0] for i in COUCHBASE_BUCKET_NAMES])
def test_object_names_bucket(sdc_builder, sdc_executor, couchbase, test_name, bucket_generator):
    """Test using different kinds of bucket names, asserting whether Couchbase receives the data"""
    bucket_name = bucket_generator()
    couchbase_host = f'{couchbase.hostname}:{couchbase.port}'
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                     raw_data=raw_data,
                                                                     stop_after_first_batch=True)

    destination = builder.add_stage('Couchbase', type='destination')
    if Version(sdc_builder.version) < Version('3.9.0'):
        destination.set_attributes(database_version='VERSION5', unique_document_key_field=document_key_field,
                                   bucket=bucket_name, couchbase_user_name=couchbase.username,
                                   couchbase_user_password=couchbase.password, url=couchbase_host)
    else:
        destination.set_attributes(authentication_mode='USER',
                                   document_key="${record:value('/" + document_key_field + "')}",
                                   bucket=bucket_name, user_name=couchbase.username,
                                   password=couchbase.password, node_list=couchbase_host)

    source >> destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.admin.bucket_create(name=bucket_name, bucket_type='couchbase', ram_quota=256)
        couchbase.wait_for_healthy_bucket(bucket_name)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        bucket = couchbase.cluster.open_bucket(bucket_name)
        doc_value = bucket.get(raw_dict[document_key_field]).value
        assert doc_value == raw_dict
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.admin.bucket_delete(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@couchbase
@pytest.mark.parametrize('batch_size', [1, 10])
def test_multiple_batches(sdc_builder, sdc_executor, couchbase, batch_size):
    couchbase_host = f'{couchbase.hostname}:{couchbase.port}'
    bucket_name = get_random_string(string.ascii_letters, 10)
    batches = 3

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Data Generator')
    source.batch_size = batch_size
    source.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq",
    }]

    destination = builder.add_stage('Couchbase', type='destination')
    if Version(sdc_builder.version) < Version('3.9.0'):
        destination.set_attributes(database_version='VERSION5', unique_document_key_field="/seq",
                                   bucket=bucket_name, couchbase_user_name=couchbase.username,
                                   couchbase_user_password=couchbase.password, url=couchbase_host)
    else:
        destination.set_attributes(authentication_mode='USER',
                                   document_key='${record:value("/seq")}',
                                   bucket=bucket_name, user_name=couchbase.username,
                                   password=couchbase.password, node_list=couchbase_host)

    wiretap = builder.add_wiretap()

    source >> destination
    source >= wiretap.destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.admin.bucket_create(name=bucket_name, bucket_type='couchbase', ram_quota=256)
        couchbase.wait_for_healthy_bucket(bucket_name)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        assert num_records == len(wiretap.output_records)

        bucket = couchbase.cluster.open_bucket(bucket_name)
        for i in range(num_records):
            assert bucket.get(str(i)).value == wiretap.output_records[i].field

    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.admin.bucket_delete(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@couchbase
def test_dataflow_events(sdc_builder, sdc_executor, couchbase):
    pytest.skip('Couchbase Destination does not produce events.')


@stub
@couchbase
def test_data_format_avro(sdc_builder, sdc_executor, couchbase):
    pass


@stub
@couchbase
def test_data_format_binary(sdc_builder, sdc_executor, couchbase):
    pass


@stub
@couchbase
def test_data_format_delimited(sdc_builder, sdc_executor, couchbase):
    pass


@couchbase
def test_data_format_json(sdc_builder, sdc_executor, couchbase):
    couchbase_host = f'{couchbase.hostname}:{couchbase.port}'
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                     raw_data=raw_data,
                                                                     stop_after_first_batch=True)

    destination = builder.add_stage('Couchbase', type='destination')
    if Version(sdc_builder.version) < Version('3.9.0'):
        destination.set_attributes(database_version='VERSION5', unique_document_key_field=document_key_field,
                                   bucket=bucket_name, couchbase_user_name=couchbase.username,
                                   couchbase_user_password=couchbase.password, url=couchbase_host, data_format='JSON')
    else:
        destination.set_attributes(authentication_mode='USER',
                                   document_key="${record:value('/" + document_key_field + "')}",
                                   bucket=bucket_name, user_name=couchbase.username,
                                   password=couchbase.password, node_list=couchbase_host, data_format='JSON')

    source >> destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.admin.bucket_create(name=bucket_name, bucket_type='couchbase', ram_quota=256)
        couchbase.wait_for_healthy_bucket(bucket_name)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        bucket = couchbase.cluster.open_bucket(bucket_name)
        doc_value = bucket.get(raw_dict[document_key_field]).value
        assert doc_value == raw_dict
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.admin.bucket_delete(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@stub
@couchbase
def test_data_format_protobuf(sdc_builder, sdc_executor, couchbase):
    pass


@stub
@couchbase
def test_data_format_sdc_record(sdc_builder, sdc_executor, couchbase):
    pass


@couchbase
def test_data_format_text(sdc_builder, sdc_executor, couchbase):
    couchbase_host = f'{couchbase.hostname}:{couchbase.port}'
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key_field = 'text'
    raw_data = 'hello'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                     raw_data=raw_data,
                                                                     stop_after_first_batch=True)

    destination = builder.add_stage('Couchbase', type='destination')
    if Version(sdc_builder.version) < Version('3.9.0'):
        destination.set_attributes(database_version='VERSION5', unique_document_key_field=document_key_field,
                                   bucket=bucket_name, couchbase_user_name=couchbase.username,
                                   couchbase_user_password=couchbase.password, url=couchbase_host, data_format='TEXT',
                                   text_field_path="/text", record_separator="\n")
    else:
        destination.set_attributes(authentication_mode='USER',
                                   document_key="${record:value('/" + document_key_field + "')}",
                                   bucket=bucket_name, user_name=couchbase.username,
                                   password=couchbase.password, node_list=couchbase_host, data_format='TEXT',
                                   text_field_path="/text", record_separator="\n")

    source >> destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.admin.bucket_create(name=bucket_name, bucket_type='couchbase', ram_quota=256)
        couchbase.wait_for_healthy_bucket(bucket_name)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info(f"Wrote {num_records} records")
        assert num_records == 1, 'Number of records stored should equal the number of records that entered the pipeline'

        bucket = couchbase.cluster.open_bucket(bucket_name)

        doc_value = bucket.get(raw_data).value
        assert doc_value == raw_data
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.admin.bucket_delete(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@stub
@couchbase
def test_push_pull(sdc_builder, sdc_executor, couchbase):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is part of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
