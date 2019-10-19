import pytest

from streamsets.testframework.decorators import stub


@stub
def test_buffer_size_in_bytes(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_compression_codec(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_data_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_dictionary_page_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_files_prefix(sdc_builder, sdc_executor):
    pass


@stub
def test_files_suffix(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_job_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_max_padding_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_page_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_rate_per_second(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_row_group_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_temporary_file_directory(sdc_builder, sdc_executor):
    pass

