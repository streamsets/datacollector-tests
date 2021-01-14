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
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_compression_codec(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'CUSTOM'}])
def test_custom_jobcreator(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_data_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_dictionary_page_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_ORC'}, {'job_type': 'AVRO_PARQUET'}])
def test_input_avro_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_job_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_job_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_ORC'},
                                              {'job_type': 'AVRO_PARQUET'},
                                              {'job_type': 'CUSTOM'},
                                              {'job_type': 'SIMPLE'}])
def test_job_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_ORC', 'keep_avro_input_file': False},
                                              {'job_type': 'AVRO_ORC', 'keep_avro_input_file': True},
                                              {'job_type': 'AVRO_PARQUET', 'keep_avro_input_file': False},
                                              {'job_type': 'AVRO_PARQUET', 'keep_avro_input_file': True}])
def test_keep_avro_input_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'kerberos_authentication': False}, {'kerberos_authentication': True}])
def test_kerberos_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_mapreduce_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_mapreduce_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
def test_mapreduce_user(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_ORC'}])
def test_orc_batch_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_ORC'}, {'job_type': 'AVRO_PARQUET'}])
def test_output_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_ORC', 'overwrite_temporary_file': False},
                                              {'job_type': 'AVRO_ORC', 'overwrite_temporary_file': True},
                                              {'job_type': 'AVRO_PARQUET', 'overwrite_temporary_file': False},
                                              {'job_type': 'AVRO_PARQUET', 'overwrite_temporary_file': True}])
def test_overwrite_temporary_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_page_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'job_type': 'AVRO_PARQUET'}])
def test_row_group_size(sdc_builder, sdc_executor, stage_attributes):
    pass

