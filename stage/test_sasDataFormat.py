import logging
import pytest
from decimal import Decimal
from streamsets.testframework.markers import sdc_min_version


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@sdc_min_version('3.8.1')
def test_sas7bdat_format(sdc_builder,sdc_executor):

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='SAS', file_name_pattern='*.sas7bdat', file_name_pattern_mode='GLOB'
                             , files_directory='/resources/tmp/')

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template='/resources/tmp/sdc/',
                            files_prefix='sdc-${sdc:id()}', files_suffix='txt', max_records_in_file=1)
    directory >> local_fs
    directory_pipeline = pipeline_builder.build('SAS7BDAT Directory Origin pipeline')
    sdc_executor.add_pipeline(directory_pipeline)
    sdc_executor.validate_pipeline(directory_pipeline)
    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)
    output_records = snapshot[directory.instance_name].output
    history = sdc_executor.get_pipeline_history(directory_pipeline)

    for record in snapshot.snapshot_batches[0][directory.instance_name].output:
        assert record.header['sourceId'] is not None
        assert record.header['stageCreator'] is not None
        assert record.header['values']['filename'] == 'test.sas7bdat'
        assert record.header['values']['baseDir'] == '/resources/tmp/'

    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 50

    if type(output_records[0].get_field_data(f'/{"/x1"}')) is dict:
        output_records[0].get_field_data(f'/{"/x1"}').get('value') == '1'
    else:
        output_records[0].get_field_data(f'/{"/x1"}') == '1'

    if type(output_records[0].get_field_data(f'/{"/x21"}')) is dict:
        output_records[0].get_field_data(f'/{"/x21"}').get('value') == '31726061'
    else:
        output_records[0].get_field_data(f'/{"/x21"}') == '31726061'




@sdc_min_version('3.8.1')
def test_sasxpt_format(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='SASXPT', file_name_pattern='*.xpt', file_name_pattern_mode='GLOB'
                             , files_directory='/resources/tmp/')

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template='/resources/tmp/sdc/',
                            files_prefix='sdc-${sdc:id()}', files_suffix='txt', max_records_in_file=1)
    directory >> local_fs
    directory_pipeline = pipeline_builder.build('SASXPT Directory Origin pipeline')
    sdc_executor.add_pipeline(directory_pipeline)
    sdc_executor.validate_pipeline(directory_pipeline)
    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)
    output_records = snapshot[directory.instance_name].output
    history = sdc_executor.get_pipeline_history(directory_pipeline)

    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 534
    for record in snapshot.snapshot_batches[0][directory.instance_name].output:
        assert record.header['sourceId'] is not None
        assert record.header['stageCreator'] is not None
        assert record.header['values']['filename'] == 'test.xpt'
        assert record.header['values']['baseDir'] == '/resources/tmp/'

    if type(output_records[0].get_field_data(f'/{"/NAME"}')) is dict:
        output_records[0].get_field_data(f'/{"/NAME"}').get('value') == 'Allanson, Andy'
    else:
        output_records[0].get_field_data(f'/{"/NAME"}') == 'Allanson, Andy'

    if type(output_records[0].get_field_data(f'/{"/TEAM"}')) is dict:
        output_records[0].get_field_data(f'/{"/TEAM"}').get('value') == 'Cleveland'
    else:
        output_records[0].get_field_data(f'/{"/TEAM"}') == 'Cleveland'


