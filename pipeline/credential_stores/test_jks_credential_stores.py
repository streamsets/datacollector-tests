# Copyright 2024 StreamSets Inc.
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
import string
from streamsets.testframework.credential_stores.jks import JKSCredentialStore
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string
from pipeline.credential_stores.credential_stores_test_set_up \
    import (_create_test_pipeline, _create_test_pipeline_to_trash, _create_and_populate_table, _check_pipeline_records,
            _drop_table, _stop_pipeline)
from streamsets.testframework.constants import CREDENTIAL_STORE_EXPRESSION
from streamsets.sdk.exceptions import RunError, RunningError, StartError, StartingError, ValidationError

logger = logging.getLogger(__name__)


@database('mariadb')
def test_jks_credential_store(sdc_builder, sdc_executor, database, credential_store):
    """
    Tests a JKS Credential Store can be used in SDC pipelines via the ${credential:get(...)} call. This test creates
    a pipeline with a JDBC Multitable Consumer and uses the credential store to provide the user and password needed to
    connect to a MariaDB database.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Wiretap
    """
    if not isinstance(credential_store, JKSCredentialStore):
        pytest.skip(f"This test only runs against Java Keystore Credential Stores")

    table_name = get_random_string(string.ascii_lowercase, 10)
    secret_name = f'test_jks_credential_store_{get_random_string(string.ascii_lowercase, 20)}'
    username_secret = f'{secret_name}_user'
    password_secret = f'{secret_name}_pass'

    pipeline = None
    connection = None

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, password_secret, database.password)
        _create_secret(credential_store, username_secret, database.username)


        pipeline, wiretap = _create_test_pipeline(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            _format_get_credential(credential_store, username_secret),
            _format_get_credential(credential_store, password_secret)
        )

        connection = _create_and_populate_table(database, table_name)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        _check_pipeline_records(wiretap.output_records)
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, password_secret)
        _delete_secret(credential_store, username_secret)
        _drop_table(connection, table_name)


@database('mariadb')
@pytest.mark.parametrize('store_id', ['skj', ''])
def test_jks_credential_store_wrong_cred_store(sdc_builder, sdc_executor, database, credential_store, store_id):
    """
    Tests that setting a JKS Credential Store indicating an incorrect credential store in the method to retrieve the
    credentials is properly detected.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Trash
    """
    if not isinstance(credential_store, JKSCredentialStore):
        pytest.skip(f"This test only runs against JKS Credential Stores")

    table_name = get_random_string(string.ascii_lowercase, 10)
    secret_name = f'test_jks_credential_store_wrong_cred_store_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline = None
    connection = None

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, secret_name, database.password)

        pipeline = _create_test_pipeline_to_trash(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            database.username,
            _format_custom_get_credential(
                store_id,
                credential_store.group_id,
                secret_name
            )
        )

        connection = _create_and_populate_table(database, table_name)
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=60)
            assert False, 'An error should have stopped the pipeline due to using an invented credential store id'
        except ValidationError as e:
            error_message = e.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            logger.info(f"Error message: {error_message}")
            assert 'CTRCMN_0100' in error_message
            assert f'Undefined \'{store_id}\' credential store' in error_message
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, secret_name)
        _drop_table(connection, table_name)


@database('mariadb')
@pytest.mark.parametrize('group_id', ['ALL', 'invented_group', ''])
def test_jks_credential_store_wrong_group(sdc_builder, sdc_executor, database, credential_store, group_id):
    """
    Tests that setting a JKS Credential Store indicating an incorrect group in the method to retrieve the credentials
    is properly detected.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Trash
    """
    if not isinstance(credential_store, JKSCredentialStore):
        pytest.skip(f"This test only runs against JKS Credential Stores")

    table_name = get_random_string(string.ascii_lowercase, 10)
    secret_name = f'test_jks_credential_store_wrong_group_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline = None
    connection = None

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, secret_name, database.password)

        pipeline = _create_test_pipeline_to_trash(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            database.username,
            _format_custom_get_credential(
                credential_store.store_id,
                group_id,
                secret_name
            )
        )

        connection = _create_and_populate_table(database, table_name)
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=60)
            assert False, 'An error should have stopped the pipeline due to using an invented group id'
        except ValidationError as e:
            error_message = e.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            logger.info(f"Error message: {error_message}")
            assert 'CREDENTIAL_STORE_001' in error_message
            assert (f'Store ID \'{credential_store.store_id}\', user does not belong to group \'{group_id}\''
                    f', cannot access credential') in error_message
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, secret_name)
        _drop_table(connection, table_name)


@database('mariadb')
def test_jks_credential_store_wrong_secret(sdc_builder, sdc_executor, database, credential_store):
    """
    Tests that setting a JKS Credential Store indicating an incorrect secret name in the method to retrieve the
    credentials is properly detected.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Wiretap
    """
    if not isinstance(credential_store, JKSCredentialStore):
        pytest.skip(f"This test only runs against JKS Credential Stores")

    table_name = get_random_string(string.ascii_lowercase, 10)
    secret_name = f'test_jks_credential_store_wrong_secret_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline = None
    connection = None

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, secret_name, database.password)

        pipeline = _create_test_pipeline_to_trash(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            database.username,
            _format_custom_get_credential(
                credential_store.store_id,
                credential_store.group_id,
                'invented_secret'
            )
        )

        connection = _create_and_populate_table(database, table_name)
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=60)
            assert False, 'An error should have stopped the pipeline due to using a non-existing secret name'
        except ValidationError as e:
            error_message = e.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            assert 'CTRCMN_0100' in error_message
            assert 'JKS_CRED_STORE_003' in error_message
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, secret_name)
        _drop_table(connection, table_name)


def _create_secret(credential_store, secret_name, secret_value):
    """
    Creates a secret named <secret_name> with the value <secret_value> in the credential_store.
    """
    logger.info(f"Creating secret '{secret_name}' in the Credential Store...")

    if _is_secret_present(credential_store, secret_name):
        assert False, f"Secret '{secret_name}' is already present in the Credential Store"

    try:
        credential_store.set_secret(secret_name, secret_value)
    except Exception as e:
        logger.error(f"Error while creating '{secret_name}' in the Credential Store..., Error : '{e}'")
        assert False, f"Failed to create secret '{secret_name}' in the Credential Store: {e}"

    assert _is_secret_present(credential_store, secret_name), \
        f"Failed to create secret '{secret_name}' in the Credential Store"


def _delete_secret(credential_store, secret_name):
    """
    Deletes the secret named <secret_name> from the credential_store.
    """
    logger.info(f"Deleting secret '{secret_name}' from the Credential Store...")
    try:
        credential_store.delete_secret(secret_name)
    except Exception as e:
        logger.info(f"Secret '{secret_name}' was not found in the Credential Store and could not be deleted")


def _format_get_credential(credential_store, secret_name):
    return CREDENTIAL_STORE_EXPRESSION.format(
        credential_store.store_id,
        credential_store.group_id,
        f'{secret_name}'
    )


def _is_secret_present(credential_store, secret_name):
    """
    Checks if there is any secret named <secret_name> in the credential_store.
    """
    try:
        secret_value = credential_store.get_latest_secret_value(secret_name)
        return True if secret_value else False
    except KeyError as e:
        return False


def _format_custom_get_credential(store_id, group_id, secret_name):
    return CREDENTIAL_STORE_EXPRESSION.format(
        store_id,
        group_id,
        f'{secret_name}'
    )
