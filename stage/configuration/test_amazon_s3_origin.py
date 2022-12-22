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

import logging

import pytest
from streamsets.testframework.markers import aws
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)

S3_SANDBOX_PREFIX = 'sandbox'
LOG_FIELD_MAPPING = [{'fieldPath': '/date', 'group': 1},
                     {'fieldPath': '/time', 'group': 2},
                     {'fieldPath': '/timehalf', 'group': 3},
                     {'fieldPath': '/info', 'group': 4},
                     {'fieldPath': '/file', 'group': 5},
                     {'fieldPath': '/message', 'group': 6}]
REGULAR_EXPRESSION = r'(\S+) (\S+) (\S+) (\S+) (\S+) (.*)'
# log to be written int the file on s3
data_format_content = {
    'COMMON_LOG_FORMAT': '127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] '
                         '"GET /apache.gif HTTP/1.0" 200 232',
    'LOG4J': '200 [main] DEBUG org.StreamSets.Log4j unknown - This is sample log message',
    'APACHE_ERROR_LOG_FORMAT': '[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client '
                               'denied by server configuration:/export/home/live/ap/htdocs/test',
    'COMBINED_LOG_FORMAT': '127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache.gif'
                           ' HTTP/1.0" 200 2326 "http://www.example.com/strt.html" "Mozilla/4.08'
                           ' [en] (Win98; I ;Nav)"',
    'APACHE_CUSTOM_LOG_FORMAT': '10.185.248.71 - - [09/Jan/2015:9:12:06 +0000] "GET '
                                '/inventoryServic/inventory/purchaseItem?userId=20253471&itemId=23434300 '
                                'HTTP/1.1" 500 17 ',
    'CEF': '10.217.31.247 CEF:0|Citrix|NetScaler|NS10.0|APPFW|APPFW_STARTURL|6|src=10.217.253.78 '
           'spt=53743 method=GET request=http://vpx247.example.net/FFC/login.html msg=Disallow Illegal URL.',
    'LEEF': 'LEEF: 2.0|Trend Micro|Deep Security Agent|<DSA version>|4000030|cat=Anti-Malware '
            'name=HEU_AEGIS_CRYPT desc=HEU_AEGIS_CRYPT sev=6 cn1=241 msg=Realtime',
    'REGEX': '2019-04-30 08:23:53 AM [INFO] [streamsets.sdk.sdc_api] Pipeline Filewriterpipeline53'}
# data to verify the output of amazon s3 origin.
get_data_to_verify_output = {
    'LOG4J': {'severity': 'DEBUG', 'relativetime': '200', 'thread': 'main', 'category': 'org.StreamSets.Log4j',
              'ndc': 'unknown', 'message': 'This is sample log message'},
    'COMMON_LOG_FORMAT': {'request': '/apache.gif', 'auth': 'frank', 'ident': '-', 'response': '200', 'bytes':
        '232', 'clientip': '127.0.0.1', 'verb': 'GET', 'httpversion': '1.0', 'rawrequest': None,
                          'timestamp': '10/Oct/2000:13:55:36 -0700'},
    'APACHE_ERROR_LOG_FORMAT': {'message': 'client denied by server configuration:/export/home/live/ap/htdocs/'
                                           'test', 'timestamp': 'Wed Oct 11 14:32:52 2000', 'loglevel': 'error',
                                'clientip': '127.0.0.1'},
    'COMBINED_LOG_FORMAT': {'request': '/apache.gif', 'agent': '"Mozilla/4.08 [en] (Win98; I ;Nav)"', 'auth':
        'frank', 'ident': '-', 'verb': 'GET', 'referrer': '"http://www.example.com/strt.'
                                                          'html"', 'response': '200', 'bytes': '2326',
                            'clientip': '127.0.0.1',
                            'httpversion': '1.0', 'rawrequest': None, 'timestamp': '10/Oct/2000:13:55:36 -0700'},
    'APACHE_CUSTOM_LOG_FORMAT': {'remoteUser': '-', 'requestTime': '09/Jan/2015:9:12:06 +0000', 'request': 'GET '
                                                                                                           '/inventoryServic/inventory/purchaseItem?userId=20253471&itemId=23434300 HTTP/1.1',
                                 'logName': '-', 'remoteHost': '10.185.248.71', 'bytesSent': '17', 'status': '500'},
    'CEF': {'severity': '6', 'product': 'NetScaler', 'extensions': {'msg': 'Disallow Illegal URL.', 'request':
        'http://vpx247.example.net/FFC/login.html', 'method': 'GET', 'src': '10.217.253.78', 'spt': '53743'},
            'signature': 'APPFW', 'vendor': 'Citrix', 'cefVersion': 0, 'name': 'APPFW_STARTURL',
            'version': 'NS10.0'},
    'GROK': {'request': '/inventoryServic/inventory/purchaseItem?userId=20253471&itemId=23434300', 'auth': '-',
             'ident': '-', 'response': '500', 'bytes': '17', 'clientip': '10.185.248.71', 'verb': 'GET',
             'httpversion': '1.1', 'rawrequest': None, 'timestamp': '09/Jan/2015:9:12:06 +0000'},
    'LEEF': {'eventId': '4000030', 'product': 'Deep Security Agent', 'extensions': {'cat': 'Realtime'},
             'leefVersion': 2.0, 'vendor': 'Trend Micro', 'version': '<DSA version>'},
    'REGEX': {'/time': '08:23:53', '/date': '2019-04-30', '/timehalf': 'AM',
              '/info': '[INFO]', '/message': 'Pipeline Filewriterpipeline53', '/file': '[streamsets.sdk.sdc_api]'}}


@pytest.mark.skip('Not yet implemented')
def test_configuration_access_key_id(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_bucket(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_connection_timeout(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('task', ['CREATE_NEW_OBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_content(sdc_builder, sdc_executor, task):
    pass


@pytest.mark.parametrize('task', ['COPY_OBJECT'])
@pytest.mark.parametrize('delete_original_object', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_delete_original_object(sdc_builder, sdc_executor, task, delete_original_object):
    pass


@pytest.mark.parametrize('region', ['OTHER'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_endpoint(sdc_builder, sdc_executor, region):
    pass


@pytest.mark.parametrize('task', ['COPY_OBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_new_object_path(sdc_builder, sdc_executor, task):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_object(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_on_record_error(sdc_builder, sdc_executor, on_record_error):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_preconditions(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('use_proxy', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_proxy_host(sdc_builder, sdc_executor, use_proxy):
    pass


@pytest.mark.parametrize('use_proxy', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_proxy_password(sdc_builder, sdc_executor, use_proxy):
    pass


@pytest.mark.parametrize('use_proxy', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_proxy_port(sdc_builder, sdc_executor, use_proxy):
    pass


@pytest.mark.parametrize('use_proxy', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_proxy_user(sdc_builder, sdc_executor, use_proxy):
    pass


@pytest.mark.parametrize('region',
                         ['AF_SOUTH_1', 'AP_EAST_1', 'AP_NORTHEAST_1', 'AP_NORTHEAST_2', 'AP_NORTHEAST_3',
                          'AP_SOUTHEAST_1', 'AP_SOUTHEAST_2', 'AP_SOUTHEAST_3', 'AP_SOUTH_1', 'AP_SOUTH_2',
                          'CA_CENTRAL_1', 'CN_NORTHWEST_1', 'CN_NORTH_1', 'EU_CENTRAL_1', 'EU_CENTRAL_2',
                          'EU_WEST_1', 'EU_WEST_2', 'EU_WEST_3', 'EU_NORTH_1', 'EU_SOUTH_1', 'EU_SOUTH_2',
                          'ME_CENTRAL_1', 'ME_SOUTH_1', 'OTHER', 'SA_EAST_1', 'US_EAST_1', 'US_EAST_2',
                          'US_GOV_EAST_1', 'US_GOV_WEST_1', 'US_WEST_1', 'US_WEST_2'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_region(sdc_builder, sdc_executor, region):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_required_fields(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_retry_count(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_secret_access_key(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_socket_timeout(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('task', ['CHANGE_EXISTING_OBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_tags(sdc_builder, sdc_executor, task):
    pass


@pytest.mark.parametrize('task', ['CHANGE_EXISTING_OBJECT', 'COPY_OBJECT', 'CREATE_NEW_OBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_task(sdc_builder, sdc_executor, task):
    pass


@pytest.mark.parametrize('use_proxy', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_use_proxy(sdc_builder, sdc_executor, use_proxy):
    pass


@aws('s3')
@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['COMMON_LOG_FORMAT', 'APACHE_ERROR_LOG_FORMAT', 'COMBINED_LOG_FORMAT',
                                        'APACHE_CUSTOM_LOG_FORMAT', 'REGEX', 'GROK', 'LOG4J', 'CEF', 'LEEF'])
def test_configurations_data_format_log(sdc_executor, sdc_builder, aws, data_format, log_format):
    """Check whether S3 origin can parse different log format or not. A log file is being created in s3 bucket
    mentioned below .S3 origin reads the log file and parse the same.

    Pipeline for the same-
    s3_origin >> trash
    s3_origin >= pipeline_finisher_executor
    """
    if log_format == 'GROK':
        file_content = data_format_content['APACHE_CUSTOM_LOG_FORMAT']
    else:
        file_content = data_format_content[log_format]
    client = aws.s3
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}'
    attributes = {'bucket': aws.s3_bucket_name,
                  'prefix_pattern': f'{s3_key}/*',
                  'read_order': 'LEXICOGRAPHICAL',
                  'data_format': data_format,
                  'log_format': log_format,
                  'custom_log_format': '%h %l %u [%t] "%r" %>s %b',
                  'regular_expression': REGULAR_EXPRESSION,
                  'field_path_to_regex_group_mapping': LOG_FIELD_MAPPING
                  }
    if Version(sdc_builder.version) >= Version('3.7.0'):
        attributes['number_of_threads'] = 1
    pipeline, wiretap = get_aws_origin_to_trash_pipeline(sdc_builder, attributes, aws)
    try:
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{get_random_string()}.log', Body=file_content)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert wiretap.output_records[0].field == get_data_to_verify_output[log_format]
    finally:
        # cleaning up s3 bucket
        delete_aws_objects(client, aws, s3_key)


@aws('s3')
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('max_record_length_in_chars', [20, 23, 30])
def test_configuration_delimited_max_record_length_in_chars(sdc_builder, sdc_executor, aws,
                                                            data_format, max_record_length_in_chars):
    """
    Case 1:   Record length > max_record_length | Expected outcome --> No records present in the wiretap
    Case 2:   Record length = max_record_length | Expected outcome --> Record processed
    Case 3:   Record length < max_record_length | Expected outcome --> Record processed
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}'
    file_name = 'file.csv'
    file_content = 'Field11,Field12,Field13'
    EXPECTED_DATA = [{'0': 'Field11', '1': 'Field12', '2': 'Field13'}]

    attributes = {'bucket': aws.s3_bucket_name,
                  'data_format': data_format,
                  'prefix_pattern': f'{s3_key}/{file_name}',
                  'max_record_length_in_chars': max_record_length_in_chars}
    pipeline, wiretap = get_aws_origin_to_trash_pipeline(sdc_builder, attributes, aws)
    amazon_s3_origin = pipeline.origin_stage
    client = aws.s3
    try:
        client.put_object(Bucket=aws.s3_bucket_name, Key=amazon_s3_origin.prefix_pattern, Body=file_content)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = [record.field for record in wiretap.output_records]

        if len(file_content) > max_record_length_in_chars:
            assert output_records == []
        else:
            assert output_records == EXPECTED_DATA
    finally:
        delete_aws_objects(client, aws, s3_key)


# Util functions
def get_aws_origin_to_trash_pipeline(sdc_builder, attributes, aws):
    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')
    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.set_attributes(**attributes)
    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> wiretap.destination
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    return s3_origin_pipeline, wiretap


def delete_aws_objects(client, aws, s3_key):
    # Clean up S3.
    delete_keys = {'Objects': [{'Key': k['Key']}
                               for k in
                               client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
    client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)
