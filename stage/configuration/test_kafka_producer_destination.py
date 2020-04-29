import logging

import pytest

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import category

from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import category, cluster, credentialstore, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ENCODED_KEYTAB_CONTENTS = 'encoded_keytab_contents'
CREDENTIAL_FUNCTION = 'credential_function'
CREDENTIAL_FUNCTION_WITH_GROUP = 'credential_function_with_group'


@pytest.fixture(autouse=True)
def kafka_check(cluster):
    if isinstance(cluster, ClouderaManagerCluster) and not hasattr(cluster, 'kafka'):
        pytest.skip('Kafka tests require Kafka to be installed on the cluster')


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_compression_codec': 'BZIP2', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'DEFLATE', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'NULL', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'SNAPPY', 'data_format': 'AVRO'}])
def test_avro_compression_codec(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE', 'data_format': 'AVRO'}])
def test_avro_schema(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'INLINE', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
def test_avro_schema_location(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': True}])
def test_basic_auth_user_info(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'BINARY'}])
def test_binary_field_path(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_broker_uri(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
def test_charset(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'checksum_algorithm': 'MD5',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'MURMUR3_128',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'MURMUR3_32',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'SHA1',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'SHA256',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'SHA512',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True}])
def test_checksum_algorithm(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'},
                                              {'data_format': 'BINARY'},
                                              {'data_format': 'DELIMITED'},
                                              {'data_format': 'JSON'},
                                              {'data_format': 'PROTOBUF'},
                                              {'data_format': 'SDC_JSON'},
                                              {'data_format': 'TEXT'},
                                              {'data_format': 'XML'}])
def test_data_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
def test_delimiter_character(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CSV'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'EXCEL'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'MULTI_CHARACTER'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'MYSQL'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'POSTGRES_CSV'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'POSTGRES_TEXT'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'RFC4180'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'TDF'}])
def test_delimiter_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
def test_escape_character(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'file_exists': 'OVERWRITE'},
                                              {'data_format': 'WHOLE_FILE', 'file_exists': 'TO_ERROR'}])
def test_file_exists(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_file_name_expression(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'include_checksum_in_events': False},
                                              {'data_format': 'WHOLE_FILE', 'include_checksum_in_events': True}])
def test_include_checksum_in_events(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'include_schema': False},
                                              {'data_format': 'AVRO', 'include_schema': True}])
def test_include_schema(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'insert_record_separator_if_no_text': False,
                                               'on_missing_field': 'IGNORE'},
                                              {'data_format': 'TEXT',
                                               'insert_record_separator_if_no_text': True,
                                               'on_missing_field': 'IGNORE'}])
def test_insert_record_separator_if_no_text(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON', 'json_content': 'ARRAY_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'MULTIPLE_OBJECTS'}])
def test_json_content(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_kafka_configuration(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'message_key_format': 'STRING'}])
def test_kafka_message_key(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'key_serializer': 'CONFLUENT', 'message_key_format': 'AVRO'},
                                              {'key_serializer': 'STRING', 'message_key_format': 'AVRO'}])
def test_key_serializer(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@category('basic')
@cluster('cdh')
@credentialstore
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('keytab_format', [ENCODED_KEYTAB_CONTENTS, CREDENTIAL_FUNCTION,
                                           CREDENTIAL_FUNCTION_WITH_GROUP])
@pytest.mark.parametrize('stage_attributes', [{'provide_keytab': True}])
def test_keytab(sdc_builder, sdc_executor, cluster, stage_attributes, keytab_format):
    test_principal(sdc_builder, sdc_executor, cluster, stage_attributes, keytab_format=keytab_format)


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
def test_lookup_schema_by(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'message_key_format': 'AVRO'}, {'message_key_format': 'STRING'}])
def test_message_key_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_message_type(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'replace_new_line_characters': True}])
def test_new_line_character_replacement(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'on_missing_field': 'ERROR'},
                                              {'data_format': 'TEXT', 'on_missing_field': 'IGNORE'}])
def test_on_missing_field(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'one_message_per_batch': False}, {'one_message_per_batch': True}])
def test_one_message_per_batch(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'partition_strategy': 'DEFAULT'}, {'partition_strategy': 'EXPRESSION'}])
def test_partition_expression(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'partition_strategy': 'DEFAULT'},
                                              {'partition_strategy': 'EXPRESSION'},
                                              {'partition_strategy': 'RANDOM'},
                                              {'partition_strategy': 'ROUND_ROBIN'}])
def test_partition_strategy(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_preconditions(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'pretty_format': False},
                                              {'data_format': 'XML', 'pretty_format': True}])
def test_pretty_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@category('basic')
@cluster('cdh')
@credentialstore
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('stage_attributes', [{'provide_keytab': True}])
def test_principal(sdc_builder, sdc_executor, cluster, stage_attributes, keytab_format=ENCODED_KEYTAB_CONTENTS):
    if not cluster.kafka.is_kerberized:
        pytest.skip('Test runs only if Kafka is kerberized')
    cloudera_streamsets = getattr(cluster, 'streamsets')
    if keytab_format in [CREDENTIAL_FUNCTION, CREDENTIAL_FUNCTION_WITH_GROUP]:
        if not cloudera_streamsets.credential_stores:
            pytest.skip('Test with credential function runs only if credential store was enabled')

    if keytab_format in [CREDENTIAL_FUNCTION_WITH_GROUP]:
        azure_keyvault = cloudera_streamsets.credential_stores.get('azure')
        if not azure_keyvault or not azure_keyvault.enforce_entry_group:
            pytest.skip('Test with credential function with enforce group runs only'
                        ' if enforceEntryGroup was set to True')

    encoded_keytabs_for_stages = getattr(cluster.kafka, 'encoded_keytabs_for_stages', None)
    keytab_for_stage = (encoded_keytabs_for_stages.get('Kafka Producer')
                        if encoded_keytabs_for_stages else None)
    if not keytab_for_stage:
        pytest.skip('Test runs only if --stage-keytab argument is provided for `Kafka Producer` stage')
    if keytab_format == ENCODED_KEYTAB_CONTENTS:
        keytab_value = keytab_for_stage.base64_encoded_keytab_contents
    elif keytab_format in [CREDENTIAL_FUNCTION, CREDENTIAL_FUNCTION_WITH_GROUP]:
        keytab_value = keytab_for_stage.credential_function_for_keytab

    # Run the pipeline and verify it works as expected.
    topic = get_random_string()
    logger.debug('Kafka topic name: %s', topic)

    DATA = ['Hello World!' for _ in range(7)]
    raw_data = '\n'.join(DATA)

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=raw_data)
    kafka_destination = builder.add_stage('Kafka Producer', library=cluster.kafka.standalone_stage_lib)

    kafka_destination.set_attributes(data_format='TEXT',
                                     keytab=keytab_value,
                                     principal=keytab_for_stage.principal,
                                     topic=topic,
                                     **stage_attributes)
    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    dev_raw_data_source >> [kafka_destination, pipeline_finisher]
    pipeline = builder.build().configure_for_environment(cluster)

    # Specify timeout so that iteration of consumer is stopped after that time and
    # specify auto_offset_reset to get messages from beginning.
    consumer = cluster.kafka.consumer(consumer_timeout_ms=5000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)

    messages = [message.value.decode().strip() for message in consumer]
    assert messages == DATA


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@category('basic')
@cluster('cdh')
@credentialstore
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('stage_attributes', [{'provide_keytab': False}, {'provide_keytab': True}])
def test_provide_keytab(sdc_builder, sdc_executor, cluster, stage_attributes):
    if stage_attributes['provide_keytab']:
        test_principal(sdc_builder, sdc_executor, cluster, stage_attributes)
    else:
        test_topic(sdc_builder, sdc_executor, cluster, stage_attributes)


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
def test_quote_character(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'quote_mode': 'ALL'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'quote_mode': 'MINIMAL'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'quote_mode': 'NONE'}])
def test_quote_mode(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
def test_record_separator(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': False},
                                              {'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': False},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': True}])
def test_register_schema(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'replace_new_line_characters': False},
                                              {'data_format': 'DELIMITED', 'replace_new_line_characters': True}])
def test_replace_new_line_characters(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_required_fields(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'response_type': 'DESTINATION_RESPONSE',
                                               'send_response_to_origin': True},
                                              {'response_type': 'SUCCESS_RECORDS', 'send_response_to_origin': True}])
def test_response_type(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'runtime_topic_resolution': False}, {'runtime_topic_resolution': True}])
def test_runtime_topic_resolution(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'}])
def test_schema_id(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
def test_schema_registry_urls(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': True}])
def test_schema_subject(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'send_response_to_origin': False}, {'send_response_to_origin': True}])
def test_send_response_to_origin(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
def test_text_field_path(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'runtime_topic_resolution': False}])
def test_topic(sdc_builder, sdc_executor, cluster, stage_attributes):
    topic = get_random_string()
    logger.debug('Kafka topic name: %s', topic)

    DATA = ['Hello World!' for _ in range(7)]
    raw_data = '\n'.join(DATA)

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=raw_data)
    kafka_destination = builder.add_stage('Kafka Producer', library=cluster.kafka.standalone_stage_lib)
    kafka_destination.set_attributes(topic=topic, data_format='TEXT', **stage_attributes)
    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    dev_raw_data_source >> [kafka_destination, pipeline_finisher]
    pipeline = builder.build().configure_for_environment(cluster)

    # Specify timeout so that iteration of consumer is stopped after that time and
    # specify auto_offset_reset to get messages from beginning.
    consumer = cluster.kafka.consumer(consumer_timeout_ms=5000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)

    messages = [message.value.decode().strip() for message in consumer]
    assert messages == DATA


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'runtime_topic_resolution': True}])
def test_topic_expression(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'runtime_topic_resolution': True}])
def test_topic_white_list(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'validate_schema': False},
                                              {'data_format': 'XML', 'validate_schema': True}])
def test_validate_schema(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'value_serializer': 'CONFLUENT'},
                                              {'data_format': 'AVRO', 'value_serializer': 'DEFAULT'}])
def test_value_serializer(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'validate_schema': True}])
def test_xml_schema(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass
