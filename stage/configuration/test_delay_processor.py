import pytest
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import category, large, sdc_min_version


@stub
@category('basic')
def test_delay_between_batches(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('advanced')
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
def test_required_fields(sdc_builder, sdc_executor):
    pass


@category('basic')
@sdc_min_version('3.20.0')
@pytest.mark.parametrize('stage_attributes', [{'skip_delay_on_empty_batch': False}, {'skip_delay_on_empty_batch': True}])
def test_skip_delay_on_empty_batch(sdc_builder, sdc_executor, stage_attributes):
    """
    Tests if there is no delay when a batch contains no records and skip_delay_on_empty_batch is true.
    There should be a delay as specified if skip_delay_on_empty_batch is false even if the record batch is empty.

    The pipeline: Dev Raw Data Source>>Delay>>Trash

    Delay processor is configured to ignore an incoming record which makes the record batch empty.
    We expect that if skip_delay_on_empty_batch is true the pipeline finishes without delays;
    or with a delay of >= ~2 secs if skip_delay_on_empty_batch is false.

    This tests checks only the behaviour of the skip_delay_on_empty_batch property.
    For stage tests please see stage/test_delay_stage.py
    """

    delta = 0.5
    min_timeout = 0 if stage_attributes['skip_delay_on_empty_batch'] else 2

    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source')
    data_source.data_format = 'JSON'
    data_source.stop_after_first_batch = True
    data_source.raw_data = '{"data": "abc"}'
    data_source.event_data = '{"data": "error"}'

    delay = builder.add_stage('Delay')
    delay.delay_between_batches = 2000
    delay.skip_delay_on_empty_batch = stage_attributes['skip_delay_on_empty_batch']
    # Ignore all incoming records to this stage, this is to give to DelayProcessor an empty batch.
    delay.preconditions = ["${false}"]
    delay.on_record_error = 'DISCARD'

    trash = builder.add_stage('Trash')

    data_source >> delay >> trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    history = sdc_executor.get_pipeline_history(pipeline)
    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
    assert min_timeout <= history.latest.metrics.timer('pipeline.batchProcessing.timer')._data.get('mean') <= min_timeout + delta
