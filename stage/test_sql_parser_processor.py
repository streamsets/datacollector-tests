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

import json
import logging
import string
import uuid

import pytest
from streamsets.sdk import sdc_api
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# SQL Parser processor was renamed in SDC-10697, so we need to reference it by name.
SQL_PARSER_STAGE_NAME = 'com_streamsets_pipeline_stage_processor_parser_sql_SqlParserDProcessor'

@pytest.mark.parametrize('case_sensitive', [True, False])
def test_sql_parser_case_sensitive(sdc_builder, sdc_executor, case_sensitive):

    """
    Check that SQL Parser Processor treats properly case-sensitiveness.
    """

    statement_sql = 'update "schema"."table" set a = 1, b = 2, A = 11, B = 21 where c = 3 and C = 31'
    statement_data = dict(statement=statement_sql)
    statement_json = json.dumps(statement_data)

    pipeline_name = f' {get_random_string(string.ascii_letters, 10)}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                              raw_data=statement_json,
                                              stop_after_first_batch=True)

    sql_parser_processor = pipeline_builder.add_stage(name=SQL_PARSER_STAGE_NAME)
    sql_parser_processor.set_attributes(sql_field='/statement',
                                        target_field='/columns',
                                        resolve_schema_from_db=False,
                                        case_sensitive_names=case_sensitive,
                                        db_time_zone='UTC')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source_origin >> sql_parser_processor >> wiretap.destination

    pipeline_title = f'SQL Parser Processor Test Pipeline: {pipeline_name}'
    pipeline = pipeline_builder.build(title=pipeline_title)
    pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    pipeline_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
    assert pipeline_status == 'FINISHED'

    for record in wiretap.output_records:
        if case_sensitive:
            assert record.field['columns']['a'] == '1'
            assert record.field['columns']['A'] == '11'
            assert record.field['columns']['b'] == '2'
            assert record.field['columns']['B'] == '21'
            assert record.field['columns']['c'] == '3'
            assert record.field['columns']['C'] == '31'
        else:
            assert not ('a' in record.field['columns'])
            assert record.field['columns']['A'] == '11'
            assert not ('b' in record.field['columns'])
            assert record.field['columns']['B'] == '21'
            assert not ('c' in record.field['columns'])
            assert record.field['columns']['C'] == '3'


def test_sql_parser_parse_exception(sdc_builder, sdc_executor):

    """
    Check that SQL Parser Processor treats wrong statements.
    """

    statement_sql = 'update "schema"."table" set a = 1, b = 2, A = 11, B ='
    statement_data = dict(statement=statement_sql)
    statement_json = json.dumps(statement_data)

    pipeline_name = f' {get_random_string(string.ascii_letters, 10)}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                              raw_data=statement_json,
                                              stop_after_first_batch=True)

    sql_parser_processor = pipeline_builder.add_stage(name=SQL_PARSER_STAGE_NAME)
    sql_parser_processor.set_attributes(sql_field='/statement',
                                        target_field='/columns',
                                        resolve_schema_from_db=False,
                                        case_sensitive_names=True,
                                        db_time_zone='UTC')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source_origin >> sql_parser_processor >> wiretap.destination

    pipeline_title = f'SQL Parser Processor Test Pipeline: {pipeline_name}'
    pipeline = pipeline_builder.build(title=pipeline_title)
    pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)

    with pytest.raises(sdc_api.RunError) as exception:
        sdc_executor.start_pipeline(pipeline)

    assert 'JDBC_96' in f'{exception.value}'
