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

