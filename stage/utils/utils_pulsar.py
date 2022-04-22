import logging
import requests
import pytest
import io
import avro.schema
import avro.io

logger = logging.getLogger(__name__)


def enforce_schema_validation_for_pulsar_topic(pulsar_admin_client, topic_name, topic_schema):
    try:
        create_topic_with_schema(pulsar_admin_client, topic_name, topic_schema)
        set_schema_validation_enforced(pulsar_admin_client)
    except requests.exceptions.HTTPError as e:
        # AFAIK there is no any way to check the pulsar server version so that we could know whether
        # it is compatible with the feature we are testing. The indirect way I found
        # up to now is to check out whether any of the API calls failed because of 404 or 405. In this
        # case just skip the test.
        if e.response.status_code in [404, 405]:
            pytest.skip(f"Error trying to create Pulsar topic and enforce schema validation."
                        f" Incompatible Pulsar: {str(e)}")
        else:
            raise e


def create_topic_with_schema(pulsar_admin_client, topic_name, topic_schema):
    pulsar_admin_client.session.headers = {'Content-type': 'application/json'}
    pulsar_admin_client.put(f"persistent/public/default/{topic_name}")
    pulsar_admin_client.post(f"schemas/public/default/{topic_name}/schema", data=topic_schema)


def set_schema_validation_enforced(pulsar_admin_client, must_be_enforced=True):
    pulsar_admin_client.session.headers = {'Content-type': 'application/json'}
    pulsar_admin_client.post(f"namespaces/public/default/schemaValidationEnforced", data=must_be_enforced)


def disable_auto_update_schema(pulsar_admin_client):
    # For previous versions < 2.5.0: There is no the isAllowAutoUpdateSchema option so that If a schema passes
    # the schema compatibility check, Pulsar producer automatically updates this schema to the topic
    # it produces by default.
    # https://pulsar.apache.org/docs/en/2.4.2/schema-manage/#schema-autoupdate
    try:
        pulsar_admin_client.session.headers = {'Content-type': 'application/json'}
        pulsar_admin_client.post(f"namespaces/public/default/isAllowAutoUpdateSchema", data=False)
        return True
    except requests.exceptions.HTTPError:
        logger.warning("API Call to set isAllowAutoUpdateSchema to False failed. Probably not supported.")
        return False


def enable_auto_update_schema(pulsar_admin_client):
    # For previous versions < 2.5.0: There is no the isAllowAutoUpdateSchema option so that If a schema passes
    # the schema compatibility check, Pulsar producer automatically updates this schema to the topic
    # it produces by default.
    # https://pulsar.apache.org/docs/en/2.4.2/schema-manage/#schema-autoupdate
    try:
        pulsar_admin_client.session.headers = {'Content-type': 'application/json'}
        pulsar_admin_client.post(f"namespaces/public/default/isAllowAutoUpdateSchema", data=True)
        return True
    except requests.exceptions.HTTPError:
        logger.warning("API Call to set isAllowAutoUpdateSchema to False failed. Probably not supported.")
        return False


def json_to_avro(json_data, avro_schema):
    bytes_writer = io.BytesIO()
    writer = avro.io.DatumWriter(avro.schema.Parse(avro_schema))
    writer.write(json_data, avro.io.BinaryEncoder(bytes_writer))
    return bytes_writer.getvalue()
