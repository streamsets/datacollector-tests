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
import string

import pytest
from streamsets.sdk.exceptions import RunError, RunningError, StartError, StartingError, ValidationError
from streamsets.testframework.constants import CREDENTIAL_STORE_EXPRESSION, CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION
from streamsets.testframework.credential_stores.aws import AWSCredentialStore
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

from pipeline.credential_stores.credential_stores_test_set_up \
    import (_create_test_pipeline, _create_and_populate_table, _check_pipeline_records, _drop_table, _stop_pipeline, \
            DEFAULT_PASSWORD_FIELD, DEFAULT_USERNAME_FIELD)

logger = logging.getLogger(__name__)

DEFAULT_OPTIONS = 'alwaysRefresh=true'


@database('mariadb')
@pytest.mark.parametrize('same_secret', [True, False])
def test_aws_credential_store(sdc_builder, sdc_executor, database, credential_store, same_secret):
    """
    Tests an AWS Credential Store can be used in SDC pipelines via the ${credential:get(...)} call. This test creates
    a pipeline with a JDBC Multitable Consumer and uses the credential store to provide the user and password needed to
    connect to a MariaDB database.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Wiretap
    """
    if not isinstance(credential_store, AWSCredentialStore):
        pytest.skip(f"This test only runs against AWS Credential Stores")

    connection = None

    table_name = get_random_string(string.ascii_lowercase, 10)

    secret_name = f'test_aws_credential_store_{get_random_string(string.ascii_lowercase, 20)}'
    username_secret = secret_name if same_secret else f'{secret_name}_user'
    password_secret = secret_name if same_secret else f'{secret_name}_pass'

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, username_secret, DEFAULT_USERNAME_FIELD, database.username)
        _create_secret(credential_store, password_secret, DEFAULT_PASSWORD_FIELD, database.password)

        pipeline, wiretap = _create_test_pipeline(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            _format_get_credential(credential_store, username_secret, DEFAULT_USERNAME_FIELD),
            _format_get_credential(credential_store, password_secret, DEFAULT_PASSWORD_FIELD)
        )

        _create_and_populate_table(database, table_name)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=60)
        sdc_executor.stop_pipeline(pipeline)

        _check_pipeline_records(wiretap.output_records)
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, username_secret, DEFAULT_USERNAME_FIELD)
        _delete_secret(credential_store, password_secret, DEFAULT_PASSWORD_FIELD)
        _drop_table(connection, table_name)


@database('mariadb')
@pytest.mark.parametrize(
    'options, separator',
    [
        ('', '&'),
        ('separator=|', '|'),
        ('separator=@', '@'),
        ('alwaysRefresh=true', '&'),
        ('alwaysRefresh=false', '&'),
        ('separator=|,alwaysRefresh=true', '|'),
    ]
)
def test_aws_credential_store_with_options(sdc_builder, sdc_executor, database, credential_store, options, separator):
    """
    Tests an AWS Credential Store can be used in SDC pipelines via the ${credential:getWithOptions(...)} call. This test
    creates a pipeline with a JDBC Multitable Consumer and uses the credential store to provide the user and password
    needed to connect to a MariaDB database.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Wiretap
    """
    if not isinstance(credential_store, AWSCredentialStore):
        pytest.skip(f"This test only runs against AWS Credential Stores")

    connection = None

    table_name = get_random_string(string.ascii_lowercase, 10)

    secret_name = f'test_aws_credential_store_with_options_{get_random_string(string.ascii_lowercase, 20)}'

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, secret_name, DEFAULT_USERNAME_FIELD, database.username)
        _create_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD, database.password)

        pipeline, wiretap = _create_test_pipeline(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            _format_get_credential_with_options(
                credential_store,
                secret_name,
                DEFAULT_USERNAME_FIELD,
                options,
                separator
            ),
            _format_get_credential_with_options(
                credential_store,
                secret_name,
                DEFAULT_PASSWORD_FIELD,
                options,
                separator
            )
        )

        _create_and_populate_table(database, table_name)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=60)
        sdc_executor.stop_pipeline(pipeline)

        _check_pipeline_records(wiretap.output_records)

        if 'alwaysRefresh=true' in options:
            _delete_secret(credential_store, secret_name, DEFAULT_USERNAME_FIELD)
            _create_secret(credential_store, secret_name, DEFAULT_USERNAME_FIELD, 'incorrect_username')
            sdc_executor.reset_origin(pipeline)

            try:
                sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=60)
                assert False, 'An error should have stopped the pipeline due to using wrong credentials'
            except (StartError, StartingError, RunError, RunningError) as e:
                assert 'JDBC_INIT_48' in e.message

    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, secret_name, DEFAULT_USERNAME_FIELD)
        _delete_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD)
        _drop_table(connection, table_name)


@database('mariadb')
@pytest.mark.parametrize('store_id', ['asw', '', 'azure', 'AWS'])
@pytest.mark.parametrize('options', [DEFAULT_OPTIONS, None])
def test_aws_credential_store_wrong_cred_store(
        sdc_builder,
        sdc_executor,
        database,
        credential_store,
        store_id,
        options
):
    """
    Tests that setting an AWS Credential Store indicating an incorrect credential store in the method to retrieve the
    credentials is properly detected. Tests both the ${credential:get(...)} and the ${credential:getWithOptions(...)}
    calls.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Wiretap
    """
    if not isinstance(credential_store, AWSCredentialStore):
        pytest.skip(f"This test only runs against AWS Credential Stores")

    table_name = get_random_string(string.ascii_lowercase, 10)
    secret_name = f'test_aws_credential_store_wrong_cred_store_{get_random_string(string.ascii_lowercase, 20)}'

    connection = None

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD, database.password)

        pipeline, wiretap = _create_test_pipeline(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            database.username,
            _format_custom_get_credential(
                store_id,
                credential_store.group_id,
                secret_name,
                DEFAULT_PASSWORD_FIELD,
                options
            )
        )

        _create_and_populate_table(database, table_name)
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=60)
            assert False, 'An error should have stopped the pipeline due to using an invented credential store id'
        except ValidationError as e:
            error_message = e.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            assert 'CTRCMN_0100' in error_message
            assert f'Undefined \'{store_id}\' credential store' in error_message
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD)
        _drop_table(connection, table_name)


@database('mariadb')
@pytest.mark.parametrize('group_id', ['ALL', 'invented_group', ''])
@pytest.mark.parametrize('options', [DEFAULT_OPTIONS, None])
def test_aws_credential_store_wrong_group(sdc_builder, sdc_executor, database, credential_store, group_id, options):
    """
    Tests that setting an AWS Credential Store indicating an incorrect group in the method to retrieve the credentials
    is properly detected. Tests both the ${credential:get(...)} and the ${credential:getWithOptions(...)} calls.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Wiretap
    """
    if not isinstance(credential_store, AWSCredentialStore):
        pytest.skip(f"This test only runs against AWS Credential Stores")

    table_name = get_random_string(string.ascii_lowercase, 10)
    secret_name = f'test_aws_credential_store_wrong_group_{get_random_string(string.ascii_lowercase, 20)}'

    connection = None

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD, database.password)

        pipeline, wiretap = _create_test_pipeline(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            database.username,
            _format_custom_get_credential(
                credential_store.store_id,
                group_id,
                secret_name,
                DEFAULT_PASSWORD_FIELD,
                options
            )
        )

        _create_and_populate_table(database, table_name)
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=60)
            assert False, 'An error should have stopped the pipeline due to using an invented group id'
        except ValidationError as e:
            error_message = e.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            assert 'CREDENTIAL_STORE_001' in error_message
            assert (f'Store ID \'{credential_store.store_id}\', user does not belong to group \'{group_id}\''
                    f', cannot access credential') in error_message
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD)
        _drop_table(connection, table_name)


@database('mariadb')
@pytest.mark.parametrize('options', [DEFAULT_OPTIONS, None])
def test_aws_credential_store_wrong_secret(sdc_builder, sdc_executor, database, credential_store, options):
    """
    Tests that setting an AWS Credential Store indicating an incorrect secret name in the method to retrieve the
    credentials is properly detected. Tests both the ${credential:get(...)} and the ${credential:getWithOptions(...)}
    calls.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Wiretap
    """
    if not isinstance(credential_store, AWSCredentialStore):
        pytest.skip(f"This test only runs against AWS Credential Stores")

    table_name = get_random_string(string.ascii_lowercase, 10)
    secret_name = f'test_aws_credential_store_wrong_secret_{get_random_string(string.ascii_lowercase, 20)}'

    connection = None

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD, database.password)

        pipeline, wiretap = _create_test_pipeline(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            database.username,
            _format_custom_get_credential(
                credential_store.store_id,
                credential_store.group_id,
                'invented_secret',
                DEFAULT_PASSWORD_FIELD,
                options
            )
        )

        _create_and_populate_table(database, table_name)
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=60)
            assert False, 'An error should have stopped the pipeline due to using a non-existing secret name'
        except ValidationError as e:
            error_message = e.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            assert 'CTRCMN_0100' in error_message
            assert 'Secrets Manager can\'t find the specified secret' in error_message
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD)
        _drop_table(connection, table_name)


@database('mariadb')
@pytest.mark.parametrize('options', [DEFAULT_OPTIONS, None])
def test_aws_credential_store_wrong_key(sdc_builder, sdc_executor, database, credential_store, options):
    """
    Tests that setting an AWS Credential Store indicating an incorrect key name in the method to retrieve the
    credentials is properly detected. Tests both the ${credential:get(...)} and the ${credential:getWithOptions(...)}
    calls.

    The pipeline created by the test looks like:
        JDBC Multitable Consumer >> Wiretap
    """
    if not isinstance(credential_store, AWSCredentialStore):
        pytest.skip(f"This test only runs against AWS Credential Stores")

    table_name = get_random_string(string.ascii_lowercase, 10)
    secret_name = f'test_aws_credential_store_wrong_key_{get_random_string(string.ascii_lowercase, 20)}'
    invented_field = 'invented_field'

    connection = None

    try:
        logger.info('Preparing the credential store secrets...')
        _create_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD, database.password)

        pipeline, wiretap = _create_test_pipeline(
            sdc_builder,
            sdc_executor,
            database,
            credential_store,
            table_name,
            database.username,
            _format_custom_get_credential(
                credential_store.store_id,
                credential_store.group_id,
                secret_name,
                invented_field,
                options
            )
        )

        _create_and_populate_table(database, table_name)
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=60)
            assert False, 'An error should have stopped the pipeline due to using a non-existing field key'
        except ValidationError as e:
            error_message = e.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']
            assert 'AWS_SECRETS_MANAGER_CRED_STORE_04' in error_message
            assert f'Key \'{invented_field}\' could not be be retrieved' in error_message
    finally:
        _stop_pipeline(sdc_executor, pipeline)
        _delete_secret(credential_store, secret_name, DEFAULT_PASSWORD_FIELD)
        _drop_table(connection, table_name)


def _create_secret(credential_store, secret_name, field, secret_value):
    """
    Creates a secret with the value <secret_value> in the credential_store as a field <field_name> for a secret named
    <secret_name>. This secret's path will be <secret_name>&<field_name>.
    """
    logger.info(f"Creating secret '{secret_name}&{field}' in the Credential Store...")

    if _is_secret_present(credential_store, secret_name, field):
        assert False, f"Secret '{secret_name}&{field}' is already present in the Credential Store"

    try:
        credential_store.set_secret(secret_name, secret_value, field)
    except Exception as e:
        assert False, f"Failed to create secret '{secret_name}&{field}' in the Credential Store: {e}"

    assert _is_secret_present(credential_store, secret_name, field), \
        f"Failed to create secret '{secret_name}&{field}' in the Credential Store"


def _delete_secret(credential_store, secret_name, field):
    """
    Deletes the secret stored in the field <field_name> for a secret named <secret_name> from the credential_store.
    """
    logger.info(f"Deleting secret '{secret_name}&{field}' from the Credential Store...")
    try:
        credential_store.delete_secret(secret_name, field)
    except KeyError:
        logger.info(f"Secret '{secret_name}&{field}' was not found in the Credential Store and could not be deleted")


def _is_secret_present(credential_store, secret_name, field):
    """
    Checks if there is any secret named <secret_name> with a field <field_name> in the credential_store.
    """
    secret_path = f'{secret_name}&{field}'
    return secret_path in credential_store.secret_name_field_values


def _format_get_credential(credential_store, secret_name, field_name):
    return CREDENTIAL_STORE_EXPRESSION.format(
        credential_store.store_id,
        credential_store.group_id,
        f'{secret_name}&{field_name}'
    )


def _format_get_credential_with_options(credential_store, secret_name, field_name, options, separator='&'):
    return CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION.format(
        credential_store.store_id,
        credential_store.group_id,
        secret_name + separator + field_name,
        options
    )


def _format_custom_get_credential(store_id, group_id, secret_name, field_name, options):
    if options:
        return CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION.format(
            store_id,
            group_id,
            f'{secret_name}&{field_name}',
            options
        )
    else:
        return CREDENTIAL_STORE_EXPRESSION.format(
            store_id,
            group_id,
            f'{secret_name}&{field_name}'
        )
