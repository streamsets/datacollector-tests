import json

import pytest
from streamsets.testframework.decorators import stub


@pytest.mark.parametrize('stage_attributes', [{'action': 'KEEP'},
                                              {'action': 'REMOVE'},
                                              {'action': 'REMOVE_CONSTANT'},
                                              {'action': 'REMOVE_EMPTY'},
                                              {'action': 'REMOVE_NULL'},
                                              {'action': 'REMOVE_NULL_EMPTY'}])
def test_action(sdc_builder, sdc_executor, stage_attributes):
    try:
        DATA = dict(name='Al Gore', birthplace='Washington, D.C.', winningYears=None, internetPatents='')

        # We'll keep the /name field.
        EXPECTED_KEEP_DATA = dict(name='Al Gore')
        # We'll remove the /name field.
        EXPECTED_REMOVE_DATA = dict(birthplace='Washington, D.C.',
                                    winningYears=None,
                                    internetPatents='')
        # We'll ask to remove all fields but set constant to his name.
        EXPECTED_REMOVE_CONSTANT_DATA = dict(birthplace='Washington, D.C.',
                                             winningYears=None,
                                             internetPatents='')
        # We'll ask to remove all fields, but only the ones that have empty string values (/internetPatents) will.
        EXPECTED_REMOVE_EMPTY_DATA = dict(name='Al Gore',
                                          birthplace='Washington, D.C.',
                                          winningYears=None)
        # We'll ask to remove all fields, but only the ones that have null values (/winningYears) will.
        EXPECTED_REMOVE_NULL_DATA = dict(name='Al Gore',
                                         birthplace='Washington, D.C.',
                                         internetPatents='')

        # We'll ask to remove all fields, but only the ones that have empty string or null values will.
        EXPECTED_REMOVE_NULL_EMPTY_DATA = dict(name='Al Gore',
                                               birthplace='Washington, D.C.')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.raw_data = json.dumps(DATA)

        field_remover = pipeline_builder.add_stage('Field Remover').set_attributes(**stage_attributes)
        if field_remover.action in ('KEEP', 'REMOVE'):
            field_remover.fields = ['/name']
        else:
            field_remover.fields = ['/name', '/birthplace', '/winningYears', '/internetPatents']
        if field_remover.action == 'REMOVE_CONSTANT':
            field_remover.constant = 'Al Gore'

        trash = pipeline_builder.add_stage('Trash')

        dev_raw_data_source >> field_remover >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[field_remover].output[0]
        assert record.field == locals()[f"EXPECTED_{field_remover.action}_DATA"]
    finally:
        sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('stage_attributes', [{'action': 'REMOVE_CONSTANT'}])
def test_constant(sdc_builder, sdc_executor, stage_attributes):
    """:py:function:`stage.configuration.test_field_remover_processor.test_action` covers this case
    as we set the remover to remove all fields, but only provide a constant that matches one."""
    pass


@stub
def test_fields(sdc_builder, sdc_executor):
    """:py:function:`stage.configuration.test_field_remover_processor.test_action` covers this case
    as we alternately set one field (when keeping or removing individual ones) or all of them."""
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

