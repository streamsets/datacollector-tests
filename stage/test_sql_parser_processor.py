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

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jdbc-lib')
    return hook

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


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('case_sensitive', [True, False])
@pytest.mark.parametrize('pseudocolumns_in_header', [True, False])
def test_sql_parser_pseudocolumns(sdc_builder, sdc_executor, case_sensitive, pseudocolumns_in_header):

    """
    Check pseudocolumns processing.
    """

    try:

        statement_sql = 'update "schema"."table" set ' \
                        'A = 11, B = 12, C  = 13, ' \
                        'NEXTVAL = 1, nextval = 2, NeXtVaL = 3, ' \
                        'ROWNUM = 4, rownum = 5, RoWnUm = 6 ' \
                        'where ' \
                        'ROWID = 7 and rowid = 8 and RoWiD = 9 and ' \
                        'd = 14 and e = 15 and f = 16'
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
                                            pseudocolumns_in_header=pseudocolumns_in_header,
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

                assert record.field['columns']['A'] == '11'
                assert record.field['columns']['B'] == '12'
                assert record.field['columns']['C'] == '13'
                assert record.field['columns']['d'] == '14'
                assert record.field['columns']['e'] == '15'
                assert record.field['columns']['f'] == '16'

                assert not ('D' in record.field['columns'])
                assert not ('E' in record.field['columns'])
                assert not ('F' in record.field['columns'])

                if pseudocolumns_in_header:

                    assert record.header.values['oracle.pseudocolumn.NEXTVAL'] == '1'
                    assert record.header.values['oracle.pseudocolumn.nextval'] == '2'
                    assert record.header.values['oracle.pseudocolumn.NeXtVaL'] == '3'
                    assert record.header.values['oracle.pseudocolumn.ROWNUM'] == '4'
                    assert record.header.values['oracle.pseudocolumn.rownum'] == '5'
                    assert record.header.values['oracle.pseudocolumn.RoWnUm'] == '6'
                    assert record.header.values['oracle.pseudocolumn.ROWID'] == '7'
                    assert record.header.values['oracle.pseudocolumn.rowid'] == '8'
                    assert record.header.values['oracle.pseudocolumn.RoWiD'] == '9'

                    assert not ('NEXTVAL' in record.field['columns'])
                    assert not ('nextval' in record.field['columns'])
                    assert not ('NeXtVaL' in record.field['columns'])
                    assert not ('ROWNUM' in record.field['columns'])
                    assert not ('rownum' in record.field['columns'])
                    assert not ('RoWnUm' in record.field['columns'])
                    assert not ('ROWID' in record.field['columns'])
                    assert not ('rowid' in record.field['columns'])
                    assert not ('RoWiD' in record.field['columns'])

                else:

                    assert not ('oracle.pseudocolumn.NEXTVAL' in record.header.values)
                    assert not ('oracle.pseudocolumn.nextval' in record.header.values)
                    assert not ('oracle.pseudocolumn.NeXtVaL' in record.header.values)
                    assert not ('oracle.pseudocolumn.ROWNUM' in record.header.values)
                    assert not ('oracle.pseudocolumn.rownum' in record.header.values)
                    assert not ('oracle.pseudocolumn.RoWnUm' in record.header.values)
                    assert not ('oracle.pseudocolumn.ROWID' in record.header.values)
                    assert not ('oracle.pseudocolumn.rowid' in record.header.values)
                    assert not ('oracle.pseudocolumn.RoWiD' in record.header.values)

                    assert record.field['columns']['NEXTVAL'] == '1'
                    assert record.field['columns']['nextval'] == '2'
                    assert record.field['columns']['NeXtVaL'] == '3'
                    assert record.field['columns']['ROWNUM'] == '4'
                    assert record.field['columns']['rownum'] == '5'
                    assert record.field['columns']['RoWnUm'] == '6'
                    assert record.field['columns']['ROWID'] == '7'
                    assert record.field['columns']['rowid'] == '8'
                    assert record.field['columns']['RoWiD'] == '9'

            else:

                assert record.field['columns']['A'] == '11'
                assert record.field['columns']['B'] == '12'
                assert record.field['columns']['C'] == '13'
                assert record.field['columns']['D'] == '14'
                assert record.field['columns']['E'] == '15'
                assert record.field['columns']['F'] == '16'

                assert not ('d' in record.field['columns'])
                assert not ('e' in record.field['columns'])
                assert not ('f' in record.field['columns'])

                if pseudocolumns_in_header:

                    assert record.header.values['oracle.pseudocolumn.NEXTVAL'] == '3'
                    assert record.header.values['oracle.pseudocolumn.ROWNUM'] == '6'
                    assert record.header.values['oracle.pseudocolumn.ROWID'] == '7'
                    assert not ('oracle.pseudocolumn.nextval' in record.header.values)
                    assert not ('oracle.pseudocolumn.NeXtVaL' in record.header.values)
                    assert not ('oracle.pseudocolumn.rownum' in record.header.values)
                    assert not ('oracle.pseudocolumn.RoWnUm' in record.header.values)
                    assert not ('oracle.pseudocolumn.rowid' in record.header.values)
                    assert not ('oracle.pseudocolumn.RoWiD' in record.header.values)

                    assert not ('NEXTVAL' in record.field['columns'])
                    assert not ('nextval' in record.field['columns'])
                    assert not ('NeXtVaL' in record.field['columns'])
                    assert not ('ROWNUM' in record.field['columns'])
                    assert not ('rownum' in record.field['columns'])
                    assert not ('RoWnUm' in record.field['columns'])
                    assert not ('ROWID' in record.field['columns'])
                    assert not ('rowid' in record.field['columns'])
                    assert not ('RoWiD' in record.field['columns'])

                else:

                    assert not ('NEXTVAL' in record.header.values)
                    assert not ('nextval' in record.header.values)
                    assert not ('NeXtVaL' in record.header.values)
                    assert not ('ROWNUM' in record.header.values)
                    assert not ('rownum' in record.header.values)
                    assert not ('RoWnUm' in record.header.values)
                    assert not ('ROWID' in record.header.values)
                    assert not ('rowid' in record.header.values)
                    assert not ('RoWiD' in record.header.values)

                    assert record.field['columns']['NEXTVAL'] == '3'
                    assert record.field['columns']['ROWNUM'] == '6'
                    assert record.field['columns']['ROWID'] == '7'
                    assert not ('nextval' in record.field['columns'])
                    assert not ('NeXtVaL' in record.field['columns'])
                    assert not ('rownum' in record.field['columns'])
                    assert not ('RoWnUm' in record.field['columns'])
                    assert not ('rowid' in record.field['columns'])
                    assert not ('RoWiD' in record.field['columns'])

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
