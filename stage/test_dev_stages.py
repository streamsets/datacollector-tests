import logging
import requests
from time import sleep

import pytest

from testframework import sdc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Assign stage name strings to variables here to allow for easy reuse across multiple tests.
DEV_RAW_DATA_SRC_ORIGIN_STAGE_NAME = 'com_streamsets_pipeline_stage_devtest_rawdata_RawDataDSource'
DEV_STAGE_LIB = 'streamsets-datacollector-dev-lib'


@pytest.fixture(scope='module')
def dc(args):
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.start()
    yield dc
    # After all tests or after an exception is encountered, stop and remove the SDC instance
    dc.tear_down()


@pytest.fixture(scope='module')
def pipeline(dc):
    pipeline = dc.get_generated_pipeline(type='origin_test',
                                         stage_name=DEV_RAW_DATA_SRC_ORIGIN_STAGE_NAME,
                                         stage_lib=DEV_STAGE_LIB)
    pipeline.stages['DevRawDataSource_01'].data_format = 'JSON'
    pipeline.stages['DevRawDataSource_01'].raw_data = "{ 'emp-id' : '123456'}"
    dc.add_pipeline(pipeline)
    yield pipeline


def test_pipeline_status(dc, pipeline):
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify running pipeline's status
    current_status = dc.api_client.get_pipeline_status(pipeline.name).response.json().get('status')
    assert current_status == 'RUNNING'

    # Stop the pipeline and verify pipeline's status
    dc.stop_pipeline(pipeline).wait_for_stopped()
    current_status = dc.api_client.get_pipeline_status(pipeline.name).response.json().get('status')
    assert current_status == 'STOPPED'


def test_pipeline_definitions(dc, pipeline):
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify definitions
    running_pipeline_definitions = dc.api_client.get_definitions()
    assert running_pipeline_definitions is not None

    # Stop the pipeline and verify definitions do not change
    dc.stop_pipeline(pipeline).wait_for_stopped()
    stopped_pipeline_definitions = dc.api_client.get_definitions()
    assert stopped_pipeline_definitions is not None
    assert running_pipeline_definitions == stopped_pipeline_definitions


def test_pipeline_metrics(dc, pipeline):
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify pipeline metrics for a running pipiline
    first_metrics_json = dc.api_client.get_pipeline_metrics(pipeline.name)
    assert first_metrics_json is not None
    sleep(15)
    second_metrics_json = dc.api_client.get_pipeline_metrics(pipeline.name)
    assert second_metrics_json is not None
    assert first_metrics_json != second_metrics_json

    # Stop the pipeline and verify stopped pipeline's metrics
    dc.stop_pipeline(pipeline).wait_for_stopped()
    assert dc.api_client.get_pipeline_metrics(pipeline.name) == {}


def test_invalid_execution_mode(dc, pipeline):
    # Set executionMode to invalid value and add that as a new pipeline
    pipeline.configuration['executionMode'] = 'Invalid_Execution_Mode'
    pipeline.name = 'Invalid_Execution_Mode Pipeline'
    dc.add_pipeline(pipeline)

    with pytest.raises(requests.exceptions.HTTPError):
        dc.start_pipeline(pipeline)
