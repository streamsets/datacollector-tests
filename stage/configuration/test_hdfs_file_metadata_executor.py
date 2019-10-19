import pytest

from streamsets.testframework.decorators import stub


@stub
def test_file_path(sdc_builder, sdc_executor):
    pass


@stub
def test_hadoop_fs_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_hadoop_fs_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
def test_hadoop_fs_uri(sdc_builder, sdc_executor):
    pass


@stub
def test_hdfs_user(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'kerberos_authentication': False}, {'kerberos_authentication': True}])
def test_kerberos_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'move_file': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'move_file': True, 'task': 'CHANGE_EXISTING_FILE'}])
def test_move_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_acls': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_acls': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_new_acls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_ownership': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_ownership': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_new_group(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'move_file': True, 'task': 'CHANGE_EXISTING_FILE'}])
def test_new_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'rename': True, 'task': 'CHANGE_EXISTING_FILE'}])
def test_new_name(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_ownership': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_ownership': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_new_owner(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_permissions': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_permissions': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_new_permissions(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'rename': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'rename': True, 'task': 'CHANGE_EXISTING_FILE'}])
def test_rename(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_acls': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_acls': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_acls': False, 'task': 'CREATE_EMPTY_FILE'},
                                              {'set_acls': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_set_acls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_ownership': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_ownership': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_ownership': False, 'task': 'CREATE_EMPTY_FILE'},
                                              {'set_ownership': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_set_ownership(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_permissions': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_permissions': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_permissions': False, 'task': 'CREATE_EMPTY_FILE'},
                                              {'set_permissions': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_set_permissions(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'CHANGE_EXISTING_FILE'},
                                              {'task': 'CREATE_EMPTY_FILE'},
                                              {'task': 'REMOVE_FILE'}])
def test_task(sdc_builder, sdc_executor, stage_attributes):
    pass

