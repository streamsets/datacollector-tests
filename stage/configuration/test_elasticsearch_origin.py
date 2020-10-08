import pytest
import string

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import elasticsearch
from streamsets.testframework.utils import get_random_string
from streamsets.sdk.exceptions import ValidationError


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True}])
def test_access_key_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_additional_http_params(sdc_builder, sdc_executor):
    pass


@stub
def test_cluster_http_uris(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'delete_scroll_on_pipeline_stop': False},
                                              {'delete_scroll_on_pipeline_stop': True}])
def test_delete_scroll_on_pipeline_stop(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'detect_additional_nodes_in_cluster': False},
                                              {'detect_additional_nodes_in_cluster': True}])
def test_detect_additional_nodes_in_cluster(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'region': 'OTHER', 'use_security': True}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': False}, {'incremental_mode': True}])
def test_incremental_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_index(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_initial_offset(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_max_batch_size(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True},
                                              {'mode': 'BASIC', 'use_security': True}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_number_of_slices(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_offset_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@elasticsearch
@pytest.mark.parametrize('test_data', [
    {'error': True, 'query': '{"query": {"matchAll": {}}}', 'error_code': 'ELASTICSEARCH_41'},
    {'error': True, 'query': 'INVALID_JSON', 'error_code': 'ELASTICSEARCH_34'},
    {'error': False, 'query': '{"query": {"match_all": {}}, "sort": ["number"]}', 'error_code': None}])
def test_query(sdc_builder, sdc_executor, elasticsearch, test_data):
    """
    We will test a valid query containing an additional field apart from the query field.
    In the validate API, only the query field is allowed, though in the search API other fields may be used,
    e.g. the sort field. Here we want to test that if a valid query contains additional fields the origin doesn't break.

    We also want to test that the validation fails if a query is invalid or a query JSON is not a valid object itself.

    The pipeline is as follows:

    Elasticsearch >> Trash

    """

    index = get_random_string(string.ascii_lowercase)
    doc_count = 10

    def generator():
        for i in range(0, doc_count):
            yield {
                "_index": index,
                "_type": "data",
                "_source": {"number": i + 1}
            }

    elasticsearch.client.bulk(generator())

    try:
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Elasticsearch', type='origin')
        origin.index = index
        origin.query = test_data['query']

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(elasticsearch)

        sdc_executor.add_pipeline(pipeline)

        if test_data['error']:
            with pytest.raises(ValidationError) as e:
                sdc_executor.validate_pipeline(pipeline)

            assert e.value.issues['issueCount'] == 1
            assert e.value.issues['stageIssues'][origin.instance_name][0]['message'].find(test_data['error_code']) != -1

        else:
            snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

            assert len(snapshot[origin].output) == doc_count

            for i in range(0, doc_count):
                record = snapshot[origin].output[i]
                assert record.field['_index'] == index
                assert record.field['_source'] == {"number": i + 1}

    finally:
        elasticsearch.client.delete_index(index)


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_query_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_3', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTHEAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTHEAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTH_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CA_CENTRAL_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CN_NORTHWEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CN_NORTH_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_CENTRAL_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_3', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'OTHER', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'SA_EAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_EAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_EAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_GOV_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_WEST_2', 'use_security': True}])
def test_region(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_scroll_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True}])
def test_secret_access_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'BASIC', 'use_security': True}])
def test_security_username_and_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': True}])
def test_ssl_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': True}])
def test_ssl_truststore_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': False}, {'use_security': True}])
def test_use_security(sdc_builder, sdc_executor, stage_attributes):
    pass

