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

import pytest
import sqlalchemy
from sqlalchemy import text
from time import sleep

from streamsets.sdk.exceptions import RunError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# SQL Parser processor was renamed in SDC-10697, so we need to reference it by name.
SQL_PARSER_STAGE_NAME = 'com_streamsets_pipeline_stage_processor_parser_sql_SqlParserDProcessor'

# Support for multithreading was added in COLLECTOR-1357, scheduled for version 5.4.0.
MULTITHREADING_SUPPORT_VERSION = '5.4.0'
THREAD_COUNT_PARAMETER = 'parsing_thread_pool_size'

SHORT_WAIT_TIME = 0
LONG_WAIT_TIME = 2000
SESSION_WAIT_TIME_MIN_VERSION = "5.3.0"


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jdbc-lib')

    return hook


@pytest.mark.parametrize('multithreading, thread_count', [(False, 1), (True, 4)])
@pytest.mark.parametrize('case_sensitive', [True, False])
def test_sql_parser_case_sensitive(sdc_builder, sdc_executor, case_sensitive, multithreading, thread_count):
    """
    Check that SQL Parser Processor treats properly case-sensitiveness.
    """

    multithreading_parameters = {}
    if multithreading:
        if Version(sdc_builder.version) < Version(MULTITHREADING_SUPPORT_VERSION):
            pytest.skip(f"Multithreading is not supported for version {sdc_builder.version}")
        else:
            multithreading_parameters = {THREAD_COUNT_PARAMETER: thread_count}

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
                                        db_time_zone='UTC',
                                        **multithreading_parameters)

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


@pytest.mark.parametrize('multithreading, thread_count', [(False, 1), (True, 4)])
def test_sql_parser_parse_exception(sdc_builder, sdc_executor, multithreading, thread_count):
    """
    Check that SQL Parser Processor treats wrong statements.
    """

    multithreading_parameters = {}
    if multithreading:
        if Version(sdc_builder.version) < Version(MULTITHREADING_SUPPORT_VERSION):
            pytest.skip(f"Multithreading is not supported for version {sdc_builder.version}")
        else:
            multithreading_parameters = {THREAD_COUNT_PARAMETER: thread_count}

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
                                        db_time_zone='UTC',
                                        **multithreading_parameters)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source_origin >> sql_parser_processor >> wiretap.destination

    pipeline_title = f'SQL Parser Processor Test Pipeline: {pipeline_name}'
    pipeline = pipeline_builder.build(title=pipeline_title)
    pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)

    with pytest.raises(RunError) as exception:
        sdc_executor.start_pipeline(pipeline)

    assert 'JDBC_96' in f'{exception.value}'


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('multithreading, thread_count', [(False, 1), (True, 4)])
@pytest.mark.parametrize('case_sensitive', [True, False])
@pytest.mark.parametrize('pseudocolumns_in_header', [True, False])
def test_sql_parser_pseudocolumns(sdc_builder,
                                  sdc_executor,
                                  case_sensitive,
                                  pseudocolumns_in_header,
                                  multithreading,
                                  thread_count):
    """
    Check pseudocolumns processing.
    """

    multithreading_parameters = {}
    if multithreading:
        if Version(sdc_builder.version) < Version(MULTITHREADING_SUPPORT_VERSION):
            pytest.skip(f"Multithreading is not supported for version {sdc_builder.version}")
        else:
            multithreading_parameters = {THREAD_COUNT_PARAMETER: thread_count}

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
                                        db_time_zone='UTC',
                                        **multithreading_parameters)

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


@sdc_min_version('4.1.0')
@database('oracle')
@pytest.mark.parametrize('multithreading, thread_count', [(False, 1), (True, 4)])
@pytest.mark.parametrize('add_unsupported_fields_to_records', [True, False])
@pytest.mark.parametrize('case_sensitive', [True, False])
@pytest.mark.parametrize('use_peg_parser', [True, False])
@pytest.mark.parametrize('pseudocolumns_in_header', [True, False])
@pytest.mark.parametrize('resolve_schema_from_db', [True, False])
@pytest.mark.parametrize('include_nulls', [True, False])
def test_sql_parser_dual_parser(sdc_builder,
                                sdc_executor,
                                database,
                                add_unsupported_fields_to_records,
                                case_sensitive,
                                use_peg_parser,
                                pseudocolumns_in_header,
                                resolve_schema_from_db,
                                include_nulls,
                                multithreading,
                                thread_count):
    """
    Check SQL Parser from CDC with several options and both SQL parsers available.
    """

    multithreading_parameters = {}
    if multithreading:
        if Version(sdc_builder.version) < Version(MULTITHREADING_SUPPORT_VERSION):
            pytest.skip(f"Multithreading is not supported for version {sdc_builder.version}")
        else:
            multithreading_parameters = {THREAD_COUNT_PARAMETER: thread_count}

    try:

        test_pattern = f'{add_unsupported_fields_to_records} - '\
                       f'{case_sensitive} - '\
                       f'{use_peg_parser} - '\
                       f'{pseudocolumns_in_header} - '\
                       f'{resolve_schema_from_db} - '\
                       f'{include_nulls}'

        logger.info(f'Running test: {test_pattern}')

        source_table = None
        target_table = None

        pipeline = None

        database_connection = database.engine.connect()

        if case_sensitive:
            source_table_name = f'{get_random_string(string.ascii_uppercase, 8)}{get_random_string(string.ascii_lowercase, 8)}'
        else:
            source_table_name = f'{get_random_string(string.ascii_uppercase, 16)}'
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(source_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('Id', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('Name', sqlalchemy.String(32)),
                                        sqlalchemy.Column('Surname', sqlalchemy.String(64)),
                                        sqlalchemy.Column('Country', sqlalchemy.String(2)),
                                        sqlalchemy.Column('City', sqlalchemy.String(3)),
                                        sqlalchemy.Column('Secret', sqlalchemy.String(10)),
                                        sqlalchemy.Column('Document', sqlalchemy.BLOB))
        source_table.create(database.engine)

        if case_sensitive:
            target_table_name = f'{get_random_string(string.ascii_uppercase, 8)}{get_random_string(string.ascii_lowercase, 8)}'
        else:
            target_table_name = f'{get_random_string(string.ascii_uppercase, 16)}'
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('Id', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('Name', sqlalchemy.String(32)),
                                        sqlalchemy.Column('Surname', sqlalchemy.String(64)),
                                        sqlalchemy.Column('Country', sqlalchemy.String(2)),
                                        sqlalchemy.Column('City', sqlalchemy.String(3)),
                                        sqlalchemy.Column('Secret', sqlalchemy.String(10)),
                                        sqlalchemy.Column('Document', sqlalchemy.BLOB))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)
        number_of_rows = 1

        database_transaction = database_connection.begin()
        for id in range(0, number_of_rows):
            table_id = id
            table_name = "'" + get_random_string(string.ascii_uppercase, 32) + "'"
            table_surname = "'" + get_random_string(string.ascii_uppercase, 64) + "'"
            table_country = "'" + get_random_string(string.ascii_uppercase, 2) + "'"
            table_city = "'" + get_random_string(string.ascii_uppercase, 3) + "'"
            table_benull = "''"
            table_beblob = "utl_raw.cast_to_raw('" + get_random_string(string.ascii_uppercase, 128) + "')"
            sentence = f'insert into "{source_table}" values ({table_id}, {table_name}, {table_surname}, {table_country}, {table_city}, {table_benull}, {table_beblob})'
            sql = text(sentence)
            database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'insert into "{target_table_name}" select * from "{source_table_name}"'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'update "{target_table_name}" set "City" = "Country"'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'delete from "{target_table_name}"'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')
        oracle_cdc_client.set_attributes(dictionary_source='DICT_FROM_ONLINE_CATALOG',
                                         tables=[{'schema': database.username.upper(),
                                                  'table': target_table_name,
                                                  'excludePattern': ''}],
                                         buffer_changes_locally=True,
                                         logminer_session_window='${10 * MINUTES}',
                                         maximum_transaction_length='${2 * MINUTES}',
                                         db_time_zone='UTC',
                                         max_batch_size_in_records=1,
                                         initial_change='SCN',
                                         start_scn=database_last_scn,
                                         case_sensitive_names=case_sensitive,
                                         include_nulls=False,
                                         parse_sql_query=False,
                                         pseudocolumns_in_header=False,
                                         send_redo_query_in_headers=True,
                                         use_peg_parser=False)

        sql_parser_processor = pipeline_builder.add_stage(name=SQL_PARSER_STAGE_NAME)
        sql_parser_processor.set_attributes(sql_field='/sql',
                                            target_field='/columns',
                                            unsupported_field_type='SEND_TO_PIPELINE',
                                            add_unsupported_fields_to_records=add_unsupported_fields_to_records,
                                            use_peg_parser=use_peg_parser,
                                            pseudocolumns_in_header=pseudocolumns_in_header,
                                            resolve_schema_from_db=resolve_schema_from_db,
                                            include_nulls=include_nulls,
                                            case_sensitive_names=case_sensitive,
                                            db_time_zone='UTC',
                                            **multithreading_parameters)

        wiretap = pipeline_builder.add_wiretap()

        oracle_cdc_client >> sql_parser_processor >> wiretap.destination

        pipeline_name = f'{test_pattern} - {get_random_string(string.ascii_letters, 8)}'
        pipeline_title = f'Oracle SQL Parser Pipeline: {pipeline_name}'
        pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(4 * number_of_rows)

        for record in wiretap.output_records:
            record_operation = record.header.values['oracle.cdc.operation']
            record_sequence = record.header.values['oracle.cdc.sequence.internal']

            debug_data = f'Debug Data: {test_pattern} - '\
                         f'{record_operation} - '\
                         f'{record_sequence} -  '\
                         f'{"Secret" in record.field["columns"]} - '\
                         f'{"SECRET" in record.field["columns"]} - '\
                         f'{"Document" in record.field["columns"]} - '\
                         f'{"DOCUMENT" in record.field["columns"]} - '\
                         f'{("oracle.pseudocolumn.ROWID" in record.header.values)} - '\
                         f'{("oracle.cdc.rowId" in record.header.values)} - '\
                         f'{("jdbc.Id.precision" in record.header.values)} - '\
                         f'{("jdbc.Id.scale" in record.header.values)} - '\
                         f'{"ROWID" in record.field["columns"]}'

            trace_data = f'Trace Data: {test_pattern} - '\
                         f'{record.header.values["oracle.cdc.scn"]} | '\
                         f'{record.header.values["oracle.cdc.sequence.internal"]} - '\
                         f'{record.header.values["oracle.cdc.sequence.oracle"]} - '\
                         f'{record.header.values["oracle.cdc.RS_ID"]} - '\
                         f'{record.header.values["oracle.cdc.SSN"]} - '\
                         f'{record.header.values["oracle.cdc.rowId"]} - '\
                         f'{record.header.values["oracle.cdc.xid"]} - '\
                         f'{record_operation} -  '\
                         f'##Field: {record.field} - '\
                         f'##Header: {record.header.values}'

            logger.info(f'{debug_data}')
            logger.info(f'{trace_data}')

            error_message = f'Unexpected value placement {debug_data} | {trace_data}'

            if case_sensitive:
                assert ('Id' in record.field['columns']), error_message
                assert ('Name' in record.field['columns']), error_message
                assert ('Surname' in record.field['columns']), error_message
                assert ('Country' in record.field['columns']), error_message
                assert ('City' in record.field['columns']), error_message
                assert not ('ID' in record.field['columns']), error_message
                assert not ('NAME' in record.field['columns']), error_message
                assert not ('SURNAME' in record.field['columns']), error_message
                assert not ('COUNTRY' in record.field['columns']), error_message
                assert not ('CITY' in record.field['columns']), error_message
            else:
                assert ('ID' in record.field['columns']), error_message
                assert ('NAME' in record.field['columns']), error_message
                assert ('SURNAME' in record.field['columns']), error_message
                assert ('COUNTRY' in record.field['columns']), error_message
                assert ('CITY' in record.field['columns']), error_message
                assert not ('Id' in record.field['columns']), error_message
                assert not ('Name' in record.field['columns']), error_message
                assert not ('Surname' in record.field['columns']), error_message
                assert not ('Country' in record.field['columns']), error_message
                assert not ('City' in record.field['columns']), error_message

            if use_peg_parser:
                if case_sensitive:
                    assert ('Secret' in record.field['columns']), error_message
                    assert not ('SECRET' in record.field['columns']), error_message
                else:
                    assert not ('Secret' in record.field['columns']), error_message
                    assert ('SECRET' in record.field['columns']), error_message
            else:
                if record_operation == 'INSERT':
                    if case_sensitive:
                        assert ('Secret' in record.field['columns']), error_message
                        assert not ('SECRET' in record.field['columns']), error_message
                    else:
                        assert not ('Secret' in record.field['columns']), error_message
                        assert ('SECRET' in record.field['columns']), error_message
                else:
                    if resolve_schema_from_db and include_nulls:
                        if case_sensitive:
                            assert ('Secret' in record.field['columns']), error_message
                            assert not ('SECRET' in record.field['columns']), error_message
                        else:
                            assert not ('Secret' in record.field['columns']), error_message
                            assert ('SECRET' in record.field['columns']), error_message
                    else:
                        assert not ('Secret' in record.field['columns']), error_message
                        assert not ('SECRET' in record.field['columns']), error_message

            if add_unsupported_fields_to_records:
                if record_operation == 'INSERT':
                    if case_sensitive:
                        assert ('Document' in record.field['columns']), error_message
                        assert not ('DOCUMENT' in record.field['columns']), error_message
                    else:
                        assert not ('Document' in record.field['columns']), error_message
                        assert ('DOCUMENT' in record.field['columns']), error_message
                else:
                    if record_operation == 'UPDATE' and record_sequence == '1':
                        if case_sensitive:
                            assert ('Document' in record.field['columns']), error_message
                            assert not ('DOCUMENT' in record.field['columns']), error_message
                        else:
                            assert not ('Document' in record.field['columns']), error_message
                            assert ('DOCUMENT' in record.field['columns']), error_message
                    else:
                        if resolve_schema_from_db and include_nulls:
                            if case_sensitive:
                                assert ('Document' in record.field['columns']), error_message
                                assert not ('DOCUMENT' in record.field['columns']), error_message
                            else:
                                assert not ('Document' in record.field['columns']), error_message
                                assert ('DOCUMENT' in record.field['columns']), error_message
                        else:
                            assert not ('Document' in record.field['columns']), error_message
                            assert not ('DOCUMENT' in record.field['columns']), error_message
            else:
                if resolve_schema_from_db:
                    assert not ('Document' in record.field['columns']), error_message
                    assert not ('DOCUMENT' in record.field['columns']), error_message
                else:
                    if record_operation == 'DELETE':
                        assert not ('Document' in record.field['columns']), error_message
                        assert not ('DOCUMENT' in record.field['columns']), error_message
                    else:
                        if record_operation == 'UPDATE' and record_sequence == '0':
                            assert not ('Document' in record.field['columns']), error_message
                            assert not ('DOCUMENT' in record.field['columns']), error_message
                        else:
                            if case_sensitive:
                                assert ('Document' in record.field['columns']), error_message
                                assert not ('DOCUMENT' in record.field['columns']), error_message
                            else:
                                assert not ('Document' in record.field['columns']), error_message
                                assert ('DOCUMENT' in record.field['columns']), error_message

            if pseudocolumns_in_header:
                if record_operation == 'INSERT':
                    assert not ('oracle.pseudocolumn.ROWID' in record.header.values), error_message
                else:
                    assert ('oracle.pseudocolumn.ROWID' in record.header.values), error_message
            else:
                assert not ('oracle.pseudocolumn.ROWID' in record.header.values), error_message

            if pseudocolumns_in_header:
                assert not ('ROWID' in record.field["columns"]), error_message
            else:
                if record_operation == 'INSERT':
                    assert not ('ROWID' in record.field["columns"]), error_message
                else:
                    assert ('ROWID' in record.field["columns"]), error_message

            if resolve_schema_from_db:
                if case_sensitive:
                    assert ('jdbc.Id.precision' in record.header.values), error_message
                    assert ('jdbc.Id.scale' in record.header.values), error_message
                    assert not ('jdbc.ID.precision' in record.header.values), error_message
                    assert not ('jdbc.ID.scale' in record.header.values), error_message
                else:
                    assert not ('jdbc.Id.precision' in record.header.values), error_message
                    assert not ('jdbc.Id.scale' in record.header.values), error_message
                    assert ('jdbc.ID.precision' in record.header.values), error_message
                    assert ('jdbc.ID.scale' in record.header.values), error_message

            assert ('oracle.cdc.scn' in record.header.values), error_message
            assert ('oracle.cdc.sequence.internal' in record.header.values), error_message
            assert ('oracle.cdc.sequence.oracle' in record.header.values), error_message
            assert ('SEQ' in record.header.values), error_message
            assert ('oracle.cdc.xid' in record.header.values), error_message
            assert ('oracle.cdc.RS_ID' in record.header.values), error_message
            assert ('oracle.cdc.SSN' in record.header.values), error_message
            assert ('oracle.cdc.undoValue' in record.header.values), error_message
            assert ('oracle.cdc.redoValue' in record.header.values), error_message
            assert ('oracle.cdc.operation' in record.header.values), error_message
            assert ('sdc.operation.type' in record.header.values), error_message
            # There was a typo in the TABLE_SCHEMA field of the debug data,
            # the key was "TABLE_SCHEM" instead of "TABLE_SCHEMA". Fixed in 5.8.0.
            table_schema_keyword = "TABLE_SCHEM" if Version(sdc_executor.version) < Version("5.8.0") else "TABLE_SCHEMA"
            assert (table_schema_keyword in record.header.values), error_message
            assert ('TABLE_NAME' in record.header.values), error_message
            assert ('oracle.cdc.table' in record.header.values), error_message
            assert ('schema' in record.header.values), error_message
            assert ('sql.table' in record.header.values), error_message
            assert ('oracle.cdc.rowId' in record.header.values), error_message
            assert ('oracle.cdc.timestamp' in record.header.values), error_message
            assert ('oracle.cdc.precisionTimestamp' in record.header.values), error_message
            assert ('oracle.cdc.user' in record.header.values), error_message
            assert ('rollback' in record.header.values), error_message
            assert ('oracle.cdc.query' in record.header.values), error_message

    finally:

        logger.info(f'Finished test: {test_pattern}')

        if pipeline is not None:
            try:
                sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
            except:
                pass

        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)




@sdc_min_version('5.1.0')
@database('oracle')
@pytest.mark.parametrize('multithreading, thread_count', [(False, 1), (True, 4)])
@pytest.mark.parametrize('add_unsupported_fields_to_records', [True, False])
@pytest.mark.parametrize('case_sensitive', [True, False])
@pytest.mark.parametrize('use_peg_parser', [True, False])
@pytest.mark.parametrize('pseudocolumns_in_header', [True, False])
@pytest.mark.parametrize('include_nulls', [True, False])
def test_sql_parser_processor_sorted_columns(sdc_builder,
                                             sdc_executor,
                                             database,
                                             add_unsupported_fields_to_records,
                                             case_sensitive,
                                             use_peg_parser,
                                             pseudocolumns_in_header,
                                             include_nulls,
                                             multithreading,
                                             thread_count):
    """
    Check that SQL Parser Processor from Oracle CDC produces columns in the database order.
    """

    multithreading_parameters = {}
    if multithreading:
        if Version(sdc_builder.version) < Version(MULTITHREADING_SUPPORT_VERSION):
            pytest.skip(f"Multithreading is not supported for version {sdc_builder.version}")
        else:
            multithreading_parameters = {THREAD_COUNT_PARAMETER: thread_count}

    try:

        test_pattern = f'{add_unsupported_fields_to_records} - '\
                       f'{case_sensitive} - '\
                       f'{use_peg_parser} - '\
                       f'{pseudocolumns_in_header} - '\
                       f'{include_nulls}'

        logger.info(f'Running test: {test_pattern}')

        source_table = None
        target_table = None

        pipeline = None

        database_connection = database.engine.connect()

        if case_sensitive:
            source_table_name = f'{get_random_string(string.ascii_uppercase, 8)}{get_random_string(string.ascii_lowercase, 8)}'
        else:
            source_table_name = f'{get_random_string(string.ascii_uppercase, 16)}'
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(source_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('A00', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('Col00', sqlalchemy.BLOB),
                                        sqlalchemy.Column('Col01', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col02', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col03', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col04', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col05', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col06', sqlalchemy.BLOB),
                                        sqlalchemy.Column('Col07', sqlalchemy.String(8)))
        source_table.create(database.engine)

        if case_sensitive:
            target_table_name = f'{get_random_string(string.ascii_uppercase, 8)}{get_random_string(string.ascii_lowercase, 8)}'
        else:
            target_table_name = f'{get_random_string(string.ascii_uppercase, 16)}'
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('A00', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('Col00', sqlalchemy.BLOB),
                                        sqlalchemy.Column('Col01', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col02', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col03', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col04', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col05', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col06', sqlalchemy.BLOB),
                                        sqlalchemy.Column('Col07', sqlalchemy.String(8)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)
        number_of_rows = 8

        database_transaction = database_connection.begin()
        for id in range(0, number_of_rows):
            table_benull = "''"
            table_beblob = "utl_raw.cast_to_raw('" + get_random_string(string.ascii_uppercase, 128) + "')"

            table_a00 = id
            table_col00 = table_beblob
            table_col01 = "'" + get_random_string(string.ascii_uppercase, 8) + "'"
            table_col02 = "'" + get_random_string(string.ascii_uppercase, 8) + "'"
            table_col03 = table_benull
            table_col04 = "'" + get_random_string(string.ascii_uppercase, 8) + "'"
            table_col05 = "'" + get_random_string(string.ascii_uppercase, 8) + "'"
            table_col06 = table_beblob
            table_col07 = "'sdc'"
            sentence = f'insert into "{source_table}" ' \
                       f'values (' \
                       f'{table_a00}, ' \
                       f'{table_col00}, ' \
                       f'{table_col01}, ' \
                       f'{table_col02}, ' \
                       f'{table_col03}, ' \
                       f'{table_col04}, ' \
                       f'{table_col05}, ' \
                       f'{table_col06},' \
                       f'{table_col07} ' \
                       f')'
            sql = text(sentence)
            database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'insert into "{target_table_name}" select * from "{source_table_name}"'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'update "{target_table_name}" set "Col03" = \'SDC\''
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'delete from "{target_table_name}" where "Col03" = \'SDC\''
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')
        oracle_cdc_client.set_attributes(dictionary_source='DICT_FROM_ONLINE_CATALOG',
                                         tables=[{'schema': database.username.upper(),
                                                  'table': target_table_name,
                                                  'excludePattern': ''}],
                                         buffer_changes_locally=True,
                                         logminer_session_window='${10 * MINUTES}',
                                         maximum_transaction_length='${2 * MINUTES}',
                                         db_time_zone='UTC',
                                         max_batch_size_in_records=1,
                                         initial_change='SCN',
                                         start_scn=database_last_scn,
                                         case_sensitive_names=case_sensitive,
                                         include_nulls=False,
                                         parse_sql_query=False,
                                         pseudocolumns_in_header=False,
                                         send_redo_query_in_headers=True,
                                         use_peg_parser=False)

        sql_parser_processor = pipeline_builder.add_stage(name=SQL_PARSER_STAGE_NAME)
        sql_parser_processor.set_attributes(sql_field='/sql',
                                            target_field='/columns',
                                            unsupported_field_type='SEND_TO_PIPELINE',
                                            add_unsupported_fields_to_records=add_unsupported_fields_to_records,
                                            use_peg_parser=use_peg_parser,
                                            pseudocolumns_in_header=pseudocolumns_in_header,
                                            resolve_schema_from_db=True,
                                            include_nulls=include_nulls,
                                            case_sensitive_names=case_sensitive,
                                            db_time_zone='UTC',
                                            **multithreading_parameters)

        wiretap = pipeline_builder.add_wiretap()

        oracle_cdc_client >> sql_parser_processor >> wiretap.destination

        pipeline_name = f'{test_pattern} - {get_random_string(string.ascii_letters, 8)}'
        pipeline_title = f'Oracle SQL Parser Pipeline: {pipeline_name}'
        pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(4 * number_of_rows, timeout_sec=300)

        assert len(wiretap.output_records) == 4 * number_of_rows

        for record in wiretap.output_records:
            last_column = ''
            logger.info(f'{record.field}')
            columns = record.field["columns"].keys()
            # Some columns might be missing due to stage configuration
            assert len(columns) >= 6
            for column in columns:
                assert column > last_column
                last_column = column

    finally:

        logger.info(f'Finished test: {test_pattern}')

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=False)

        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@sdc_min_version('5.1.0')
@database('oracle')
@pytest.mark.parametrize('multithreading, thread_count', [(False, 1), (True, 4)])
@pytest.mark.parametrize('use_peg_parser', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_decimal_attributes(sdc_builder,
                            sdc_executor,
                            database,
                            use_peg_parser,
                            buffer_location,
                            multithreading,
                            thread_count):
    """Validates that Field attributes for decimal types will get properly generated
    """

    db_engine = database.engine
    pipeline = None
    table = None

    multithreading_parameters = {}
    if multithreading:
        if Version(sdc_builder.version) < Version(MULTITHREADING_SUPPORT_VERSION):
            pytest.skip(f"Multithreading is not supported for version {sdc_builder.version}")
        else:
            multithreading_parameters = {THREAD_COUNT_PARAMETER: thread_count}

    try:
        table_name = get_random_string(string.ascii_uppercase, 9)
        logger.info('Using table pattern %s', table_name)

        db_connection = database.engine.connect()
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('QUANTITY', sqlalchemy.Numeric(20, 2)))
        table.create(db_engine)

        db_last_scn = _get_last_scn(db_connection)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')
        oracle_cdc_client.set_attributes(dictionary_source='DICT_FROM_ONLINE_CATALOG',
                                         tables=[{'schema': database.username.upper(),
                                                  'table': table_name,
                                                  'excludePattern': ''}],
                                         buffer_changes_locally=True,
                                         buffer_location=buffer_location,
                                         logminer_session_window='${10 * MINUTES}',
                                         maximum_transaction_length='${2 * MINUTES}',
                                         db_time_zone='UTC',
                                         max_batch_size_in_records=1,
                                         initial_change='SCN',
                                         start_scn=db_last_scn,
                                         parse_sql_query=False,
                                         use_peg_parser=False,
                                         case_sensitive_names=False,
                                         include_nulls=False,
                                         pseudocolumns_in_header=False,
                                         send_redo_query_in_headers=True)

        sql_parser_processor = pipeline_builder.add_stage(name=SQL_PARSER_STAGE_NAME)
        sql_parser_processor.set_attributes(sql_field='/sql',
                                            target_field='/columns',
                                            db_time_zone='UTC',
                                            use_peg_parser=use_peg_parser,
                                            resolve_schema_from_db=True,
                                            unsupported_field_type='SEND_TO_PIPELINE',
                                            add_unsupported_fields_to_records=True,
                                            pseudocolumns_in_header=True,
                                            include_nulls=True,
                                            case_sensitive_names=False,
                                            **multithreading_parameters)

        wiretap = pipeline_builder.add_wiretap()

        lines = [
            f'insert into {table_name} values (1, 10.2)',
        ]
        txn = db_connection.begin()
        for line in lines:
            transaction_text = text(line)
            db_connection.execute(transaction_text)
        txn.commit()

        oracle_cdc_client >> sql_parser_processor >> wiretap.destination
        pipeline = pipeline_builder.build('SQL Parser Processor: Decimal Attributes').configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)

        assert len(wiretap.output_records) == 1

        assert '20' == wiretap.output_records[0].field["columns"]["QUANTITY"].attributes['precision']
        assert '2' == wiretap.output_records[0].field["columns"]["QUANTITY"].attributes['scale']

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=False)

        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', table_name)


@sdc_min_version('5.6.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('use_peg_parser', [True, False])
def test_sql_parser_processor_primary_keys_headers(sdc_builder,
                                                   sdc_executor,
                                                   database,
                                                   buffer_location,
                                                   use_peg_parser):
    """
    Test to check all headers for primary keys are present in the output records.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    multithreading_parameters = {}

    pipeline = None

    try:

        database_connection = database.engine.connect()

        table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('TYPE', sqlalchemy.String(64), primary_key=True),
                                 sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('NAME', sqlalchemy.String(64)),
                                 sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                 sqlalchemy.Column('ADDRESS', sqlalchemy.String(64)))
        table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)

        column_type = "'" + "Hobbit" + "'"
        column_id = 1
        column_name = "'" + "Bilbo" + "'"
        column_surname = "'" + "Baggins" + "'"

        column_address = "'" + "Bag End 0" + "'"
        database_transaction = database_connection.begin()
        sentence = f"insert into {table} " \
                   f"values ({column_type}, {column_id}, {column_name}, {column_surname}, {column_address})"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 1" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Fallohide' where TYPE = 'Hobbit' and ID = 1"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 2" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, ID = 2 where TYPE = 'Fallohide' and ID = 1"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 3" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Hobbit - Fallohide', ID = 3 where TYPE = 'Fallohide' and ID = 2"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 4" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Hobbit, Fallohide' where TYPE = 'Hobbit - Fallohide'"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 5" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, ID = 4 where ID = 3"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 6" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address} where ID = 4"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f"delete from {table_name}"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(buffer_changes_locally=True,
                                         buffer_location=buffer_location,
                                         case_sensitive_names=False,
                                         db_time_zone="UTC",
                                         include_nulls=False,
                                         initial_change="SCN",
                                         dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         disable_continuous_mine=True,
                                         logminer_session_window="${2 * MINUTES}",
                                         max_batch_size_in_records=8,
                                         maximum_transaction_length="${1 * MINUTES}",
                                         parse_sql_query=False,
                                         pseudocolumns_in_header=False,
                                         send_redo_query_in_headers=True,
                                         start_scn=database_last_scn,
                                         tables=[{"schema": database.username.upper(),
                                                  "table": table_name,
                                                  "excludePattern": ""}],
                                         use_peg_parser=use_peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        sql_parser_processor = pipeline_builder.add_stage(name=SQL_PARSER_STAGE_NAME)
        sql_parser_processor.set_attributes(add_unsupported_fields_to_records=True,
                                            case_sensitive_names=False,
                                            db_time_zone='UTC',
                                            include_nulls=True,
                                            pseudocolumns_in_header=True,
                                            resolve_schema_from_db=True,
                                            sql_field='/sql',
                                            target_field='/columns',
                                            unsupported_field_type='SEND_TO_PIPELINE',
                                            use_peg_parser=use_peg_parser,
                                            **multithreading_parameters)

        wiretap = pipeline_builder.add_wiretap()

        oracle_cdc_client >> sql_parser_processor >> wiretap.destination

        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 8, timeout_sec=180)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = 8
        wiretap_output_records = wiretap.output_records
        while len(wiretap_output_records) != wiretap_output_records_control_length and \
                wiretap_output_records_retries < wiretap_output_records_max_retries:
            wiretap_output_records_retries = wiretap_output_records_retries + 1
            logger.info(
                f'wiretap says it has {wiretap_output_records_control_length} records, but it actually has {len(wiretap_output_records)} records')
            logger.info(
                f'waiting {wiretap_output_records_max_wait} seconds ({wiretap_output_records_retries} out of {wiretap_output_records_max_retries} retry)')
            sleep(wiretap_output_records_max_wait)
            wiretap_output_records = wiretap.output_records

        assert len(wiretap_output_records) == 8

        for record in wiretap_output_records:
            assert "schema" in record.header.values
            assert "oracle.cdc.table" in record.header.values
            assert "oracle.cdc.operation" in record.header.values
            assert "sdc.operation.type" in record.header.values
            assert "oracle.cdc.redoValue" in record.header.values
            assert "oracle.cdc.undoValue" in record.header.values
            assert "oracle.cdc.query" in record.header.values
            assert {record.header.values["schema"]} is not None
            assert {record.header.values["oracle.cdc.table"]} is not None
            assert {record.header.values["oracle.cdc.operation"]} is not None
            assert {record.header.values["sdc.operation.type"]} is not None
            assert {record.header.values["oracle.cdc.redoValue"]} is not None
            assert {record.header.values["oracle.cdc.undoValue"]} is not None
            assert {record.header.values["oracle.cdc.query"]} is not None
            if record.header.values["oracle.cdc.operation"] == 'UPDATE':

                assert "jdbc.primaryKey.before.TYPE" in record.header.values
                assert "jdbc.primaryKey.before.ID" in record.header.values
                assert "jdbc.primaryKey.after.TYPE" in record.header.values
                assert "jdbc.primaryKey.after.ID" in record.header.values

                assert record.header.values["jdbc.primaryKey.before.TYPE"] is not None
                assert record.header.values["jdbc.primaryKey.before.ID"] is not None
                assert record.header.values["jdbc.primaryKey.after.TYPE"] is not None
                assert record.header.values["jdbc.primaryKey.after.ID"] is not None

                column_address = record.field['columns']['ADDRESS'].value

                if column_address == 'Bag End 1':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Hobbit"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "1"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "1"
                elif column_address == 'Bag End 2':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "1"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "2"
                elif column_address == 'Bag End 3':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "2"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Hobbit - Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "3"
                elif column_address == 'Bag End 4':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Hobbit - Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "3"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "3"
                elif column_address == 'Bag End 5':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "3"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "4"
                elif column_address == 'Bag End 6':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "4"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "4"

            else:

                assert "jdbc.primaryKey.before.TYPE" not in record.header.values
                assert "jdbc.primaryKey.before.ID" not in record.header.values
                assert "jdbc.primaryKey.after.TYPE" not in record.header.values
                assert "jdbc.primaryKey.after.ID" not in record.header.values

            logger.info(f"schema..............: {record.header.values['schema']}")
            logger.info(f"oracle.cdc.table....: {record.header.values['oracle.cdc.table']}")
            logger.info(f"oracle.cdc.operation: {record.header.values['oracle.cdc.operation']}")
            logger.info(f"sdc.operation.type..: {record.header.values['sdc.operation.type']}")
            logger.info(f"oracle.cdc.redoValue: {record.header.values['oracle.cdc.redoValue']}")
            logger.info(f"oracle.cdc.undoValue: {record.header.values['oracle.cdc.undoValue']}")
            logger.info(f"oracle.cdc.query....: {record.header.values['oracle.cdc.query']}")
            logger.info(f".....................")
            if record.header.values["oracle.cdc.operation"] == 'UPDATE':
                logger.info(f"column - address.................: {record.field['columns']['ADDRESS'].value}")
                logger.info(f"jdbc.primaryKey.before.TYPE: {record.header.values['jdbc.primaryKey.before.TYPE']}")
                logger.info(f"jdbc.primaryKey.before.ID..: {record.header.values['jdbc.primaryKey.before.ID']}")
                logger.info(f"jdbc.primaryKey.after.TYPE.: {record.header.values['jdbc.primaryKey.after.TYPE']}")
                logger.info(f"jdbc.primaryKey.after.ID...: {record.header.values['jdbc.primaryKey.after.ID']}")
                logger.info(f"----------------------------------")

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if table is not None:
            table.drop(database.engine)


def set_session_wait_times(sdc_builder, oracle_cdc_client_stage, wait_time=SHORT_WAIT_TIME):
    if Version(sdc_builder.version) >= Version(SESSION_WAIT_TIME_MIN_VERSION):
        oracle_cdc_client_stage.set_attributes(time_after_session_window_start_in_ms=wait_time,
                                               time_between_session_windows_in_ms=wait_time)


def _get_last_scn(connection):

    """
    Obtains last SCN from the database or raises an Exception if anything wrong happened.
    """
    try:
        return str(connection.execute('select CURRENT_SCN from V$DATABASE').first()[0])
    except:
        raise Exception('Error retrieving last SCN from Oracle database.')
