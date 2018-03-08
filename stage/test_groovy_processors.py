# Copyright 2017 StreamSets Inc.
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

import pytest
from streamsets.testframework import sdc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# pylint: disable=pointless-statement, redefined-outer-name, too-many-locals


@pytest.fixture(scope='module')
def sdc_builder(args):
    """Create specific sdc_builder for this test which will load Groovy stage lib. Note: this one will override
    STF conftest sdc_builder.
    """
    data_collector = sdc.DataCollector(version=args.pre_upgrade_sdc_version or args.sdc_version,
                                       username=args.sdc_username,
                                       password=args.sdc_password,
                                       server_url=args.sdc_server_url,
                                       tear_down_on_exit=not args.keep_sdc_instances,
                                       always_pull=args.sdc_always_pull,
                                       enable_kerberos=args.kerberos)

    # Load Groovy stage lib required for tests of this module.
    data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')
    data_collector.start()

    yield data_collector

    if data_collector.tear_down_on_exit:
        data_collector.tear_down()


@pytest.fixture(scope='module')
def sdc_executor(args, sdc_builder):
    """Create specific sdc_executor for this test which will load Groovy stage lib. Note: this one will override
    STF conftest sdc_executor.
    """
    if args.pre_upgrade_sdc_version != args.post_upgrade_sdc_version:
        data_collector = sdc.DataCollector(version=args.post_upgrade_sdc_version,
                                           username=args.sdc_username,
                                           password=args.sdc_password,
                                           server_url=args.sdc_server_url,
                                           tear_down_on_exit=not args.keep_sdc_instances,
                                           always_pull=args.sdc_always_pull,
                                           enable_kerberos=args.kerberos)
        # Load Groovy stage lib required for tests of this module.
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')
        data_collector.start()

        yield data_collector

        if data_collector.tear_down_on_exit:
            data_collector.tear_down()
    else:
        yield sdc_builder


def test_groovy_evaluator(sdc_builder, sdc_executor):
    """Test Groovy Evaluator processor. We test by setting up a template object in initialization script, then
    main processing script which access the template to create a Groovy object per record and to create a new record
    attribute. Finally in destroy script, we collate all the records for its new attribute and Groovy assert its value.
    The pipeline would look like:

        dev_raw_data_source >> groovy_evaluator >> trash
    """
    raw_company_1 = dict(name='StreamSets', floors=3)
    raw_company_2 = dict(name='Example Inc.', floors=1)
    raw_company_3 = dict(name='ASDF', floors=10)
    raw_company_4 = dict(name='QWER Inc.', floors=2)
    raw_data = json.dumps([raw_company_1, raw_company_2, raw_company_3, raw_company_4])

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data)
    groovy_evaluator = pipeline_builder.add_stage('Groovy Evaluator', type='processor')
    # in the init script we create a 'Building' template object which can be cloned per each pipeline record processing
    init_script = """
        import groovy.transform.*

        @Canonical
        class Building implements Cloneable{
            String name
            int floors
            boolean officeSpace
        }

        state['building_template'] = new Building()
    """
    # in the script for every record processing, we compute if a building has more than 2 floors and
    # if so we create a record attribute 'officeSpace' to 'true' else 'false'
    script = """
        state['buildings'] = []
        for (record in records) {
          try {
            record.value['officeSpace'] = (record.value['floors'] > 2) ?: false

            def building = state['building_template'].clone()
            building.name = record.value['name']
            building.floors = record.value['floors']
            building.officeSpace = record.value['officeSpace']
            state['buildings'] << building
            output.write(record)
          } catch (e) {
            log.error(e.toString(), e)
            error.write(record, e.toString())
          }
        }
    """
    # in the destroy script, we collate all the building names which have 'officeSpace' and do a Groovy assert
    destroy_script = """
        def buildings = state['buildings']
        def mapBuildingName = { building -> building.name }
        def officeBuildingNames =
            buildings
                .stream()
                .filter { building ->
                    building.officeSpace && building.floors > 2
                }
                .map(mapBuildingName)
                .collect()

        assert officeBuildingNames == ['StreamSets', 'ASDF']
    """
    groovy_evaluator.set_attributes(destroy_script=destroy_script, enable_invokedynamic_compiler_option=True,
                                    init_script=init_script, record_processing_mode='BATCH', script=script)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> groovy_evaluator >> trash
    pipeline = pipeline_builder.build('Groovy Evaluator pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    output_records = snapshot[groovy_evaluator.instance_name].output  # is a list of output records
    # search for a record whose 'name' is raw_company_1['name'] and assert new attribute ('officeSpace') is created
    # with expected boolean value (where 'floors' > 2)
    record_1 = list(filter(lambda x: x.value['value']['name']['value'] == raw_company_1['name'], output_records))[0]
    assert record_1.value['value']['officeSpace']['value'] == (raw_company_1['floors'] > 2)
    record_2 = list(filter(lambda x: x.value['value']['name']['value'] == raw_company_2['name'], output_records))[0]
    assert record_2.value['value']['officeSpace']['value'] == (raw_company_2['floors'] > 2)
