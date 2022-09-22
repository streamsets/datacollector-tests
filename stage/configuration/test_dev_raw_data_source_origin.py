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
import textwrap
import string

import pytest

from streamsets.testframework.decorators import stub
from streamsets.testframework.utils import get_random_string
from streamsets.sdk.exceptions import ValidationError
from streamsets.testframework.markers import sdc_min_version


@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': False,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'}])
def test_allow_extra_columns(sdc_builder, sdc_executor, stage_attributes):
    """Depending on whether Allow Extra Columns is enabled, origin either handles records with an
    unexpected number of columns or sends such records to error while sending compliant records to output.
    """
    MESSAGE = textwrap.dedent("""\
                              column1,column2,column3
                              Field11,Field12,Field13,Field14,Field15
                              Field21,Field22,Field23
                              """)
    EXPECTED_OUTPUT_ALLOW_EXTRA_COLUMNS = [{'column1': 'Field11',
                                            'column2': 'Field12',
                                            'column3': 'Field13',
                                            '_extra_01': 'Field14',
                                            '_extra_02': 'Field15'},
                                           {'column1': 'Field21',
                                            'column2': 'Field22',
                                            'column3': 'Field23'}]
    EXPECTED_OUTPUT_DISALLOW_EXTRA_COLUMNS = [{'column1': 'Field21',
                                               'column2': 'Field22',
                                               'column3': 'Field23'}]
    CANNOT_PARSE_RECORD_ERROR_CODE = 'DELIMITED_PARSER_01'
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(raw_data=MESSAGE,
                                                                                           stop_after_first_batch=True,
                                                                                           **stage_attributes)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    dev_raw_data_source >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED')
    if stage_attributes['allow_extra_columns']:
        assert [record.field for record in wiretap.output_records] == EXPECTED_OUTPUT_ALLOW_EXTRA_COLUMNS
    else:
        assert (len(wiretap.error_records) == 1
                and CANNOT_PARSE_RECORD_ERROR_CODE in wiretap.error_records[0].header['errorMessage'])
        assert [record.field for record in wiretap.output_records] == EXPECTED_OUTPUT_DISALLOW_EXTRA_COLUMNS

@sdc_min_version('5.3.0')
def test_missing_file(sdc_builder, sdc_executor, shell_executor):
    """Check for proper error when file do not exist using runtime:loadResource().
    """
    random_str = get_random_string(string.ascii_letters, 10)
    MESSAGE = textwrap.dedent("${runtime:loadResource(\"" + random_str + "\",false)}")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(raw_data=MESSAGE,
                                                                                           stop_after_first_batch=True)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.validate_pipeline(pipeline)
        assert False, 'Should not reach here. File "' + random_str + "' should not exist."
    except ValidationError as error:
        assert "CTRCMN_0100" in error.issues

@sdc_min_version('5.3.0')
def test_raw_data_is_empty(sdc_builder, sdc_executor):
    """
    Verify that error DEV_002 is thrown when Raw Data field is empty.

    The pipeline looks like:
      dev_raw_data_source >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(raw_data="",
                                                                                           stop_after_first_batch=False)
    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> trash
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.validate_pipeline(pipeline)
        pytest.fail('This point should not be reached')
    except ValidationError as error:
        assert error.issues['issueCount'] == 1
        assert 'DEV_002' in error.issues['stageIssues']["DevRawDataSource_01"][0]['message']

@stub
def test_event_data(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_raw_data(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'stop_after_first_batch': False}, {'stop_after_first_batch': True}])
def test_stop_after_first_batch(sdc_builder, sdc_executor, stage_attributes):
    pass

