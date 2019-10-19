import pytest

from streamsets.testframework.decorators import stub


@stub
def test_admin_operation_timeout_in_milliseconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'change_log_format': 'MSSQL'},
                                              {'change_log_format': 'MongoDBOpLog'},
                                              {'change_log_format': 'MySQLBinLog'},
                                              {'change_log_format': 'NONE'},
                                              {'change_log_format': 'OracleCDC'}])
def test_change_log_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'default_operation': 'DELETE'},
                                              {'default_operation': 'INSERT'},
                                              {'default_operation': 'UPDATE'},
                                              {'default_operation': 'UPSERT'}])
def test_default_operation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'external_consistency': 'CLIENT_PROPAGATED'},
                                              {'external_consistency': 'COMMIT_WAIT'}])
def test_external_consistency(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_to_column_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_kudu_masters(sdc_builder, sdc_executor):
    pass


@stub
def test_maximum_number_of_worker_threads(sdc_builder, sdc_executor):
    pass


@stub
def test_mutation_buffer_space_in_records(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_operation_timeout_in_milliseconds(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'unsupported_operation_handling': 'DISCARD'},
                                              {'unsupported_operation_handling': 'SEND_TO_ERROR'},
                                              {'unsupported_operation_handling': 'USE_DEFAULT'}])
def test_unsupported_operation_handling(sdc_builder, sdc_executor, stage_attributes):
    pass

