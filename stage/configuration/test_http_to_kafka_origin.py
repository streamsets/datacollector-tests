import pytest

from streamsets.sdk.sdc_api import StartError
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import cluster


KEYSTORE_FILE_PATH = 'resources/tls/keystore.jks'
KEYSTORE_TYPE = 'JKS'
KEYSTORE_PASSWORD = 'password'


@stub
def test_application_id(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'application_id_in_url': False}, {'application_id_in_url': True}])
def test_application_id_in_url(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_broker_uri(sdc_builder, sdc_executor):
    pass


@stub
def test_charset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True}])
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_http_listening_port(sdc_builder, sdc_executor):
    pass


@stub
def test_kafka_configuration(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'message_key_format': 'AVRO'}, {'message_key_format': 'STRING'}])
def test_kafka_message_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'key_serializer': 'CONFLUENT', 'message_key_format': 'AVRO'},
                                              {'key_serializer': 'STRING', 'message_key_format': 'AVRO'}])
def test_key_serializer(sdc_builder, sdc_executor, stage_attributes):
    pass


@cluster('kafka', 'cdh', 'hdp')
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True, 'keystore_file': KEYSTORE_FILE_PATH},
                                              {'use_tls': True, 'keystore_file': 'wrong/path/file.jks'}])
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes, cluster):
    """Test "KeyStore path" config parameter. It is tested with two values, one pointing to a real KeyStore file
    and the other to an unexisting file. We check a TLS_01 error is raised for the unexisting file and that
    the pipeline successfully transitions to RUNNING state if the file exists.

    Pipeline:
      http_kafka >> trash

    """
    builder = sdc_builder.get_pipeline_builder()
    http_kafka = builder.add_stage('HTTP to Kafka')
    http_kafka.set_attributes(keystore_type=KEYSTORE_TYPE,
                              keystore_password=KEYSTORE_PASSWORD,
                              application_id='admin',
                              **stage_attributes)
    trash = builder.add_stage('Trash')
    http_kafka >> trash

    pipeline = builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    if stage_attributes['keystore_file'] == KEYSTORE_FILE_PATH:
        # Expecting SDC loads the KeyStore and successfully starts to run the pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_status(status='RUNNING')
        sdc_executor.stop_pipeline(pipeline)
    else:
        # Expecting a StartError from SDC due to unexisting KeyStore file (TLS_01 error).
        with pytest.raises(StartError) as e:
            sdc_executor.start_pipeline(pipeline).wait_for_status(status='RUNNING')
        assert e.value.message.startswith('TLS_01')


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_tls': True},
                                              {'keystore_type': 'PKCS12', 'use_tls': True}])
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_concurrent_requests(sdc_builder, sdc_executor):
    pass


@stub
def test_max_message_size_in_kb(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'message_key_format': 'AVRO'}, {'message_key_format': 'STRING'}])
def test_message_key_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_missing_field': 'ERROR'}, {'on_missing_field': 'IGNORE'}])
def test_on_missing_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'pretty_format': False}, {'pretty_format': True}])
def test_pretty_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'quote_mode': 'ALL'}, {'quote_mode': 'MINIMAL'}, {'quote_mode': 'NONE'}])
def test_quote_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{}, {}])
def test_topic(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True}])
def test_transport_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True},
                                              {'use_default_cipher_suites': True, 'use_tls': True}])
def test_use_default_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True},
                                              {'use_default_protocols': True, 'use_tls': True}])
def test_use_default_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'validate_schema': False}, {'validate_schema': True}])
def test_validate_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'value_serializer': 'CONFLUENT'}, {'value_serializer': 'DEFAULT'}])
def test_value_serializer(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'validate_schema': True}])
def test_xml_schema(sdc_builder, sdc_executor, stage_attributes):
    pass
