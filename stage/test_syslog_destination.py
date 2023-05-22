# Copyright 2023 StreamSets Inc.
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


def test_syslog_write_single_record(sdc_builder, sdc_executor):
    """Writes a single record to syslog destination.To achieve testing this, we have two pipelines. The 1st one will
    write data to syslog destination using UDP protocol at port 514 and 2nd will read the data using UDP Source origin
    listening at 514 port. We then use wiretap on the 2nd pipeline to assert data. The pipelines looks like:

        dev_raw_data_source >> syslog_destination
    and
        udp_source >> wiretap.destination
    """
    # 1st pipeline
    raw_data = ('{"text":"myTestMessage",'
                  '"hostname":"localhost",'
                  '"application":"myTestApp",'
                  '"facility":"1",'
                  '"severity":"1"}')

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    syslog_destination = pipeline_builder.add_stage('Syslog')
    syslog_destination.set_attributes(syslog_host='localhost',
                                      syslog_port=514,
                                      message_format='RFC_5424',
                                      protocol='UDP',
                                      timestamp="${time:now()}",
                                      hostname="${record:value('/hostname')}",
                                      severity_level="${record:value('/severity')}",
                                      syslog_facility="${record:value('/facility')}",
                                      application_name="${record:value('/application')}",
                                      data_format='JSON')

    dev_raw_data_source >> syslog_destination
    syslog_pipeline = pipeline_builder.build('Syslog Write Single Record pipeline')
    sdc_executor.add_pipeline(syslog_pipeline)

    #2nd pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()

    udp_source = pipeline_builder.add_stage('UDP Source')
    udp_source.set_attributes(data_format='SYSLOG',
                              port=["514"])

    wiretap = pipeline_builder.add_wiretap()
    udp_source >> wiretap.destination

    udp_pipeline = pipeline_builder.build('UDP Read Syslog pipeline')
    sdc_executor.add_pipeline(udp_pipeline)

    try:
        sdc_executor.start_pipeline(udp_pipeline)
        sdc_executor.start_pipeline(syslog_pipeline).wait_for_finished()
        sdc_executor.wait_for_pipeline_metric(udp_pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(udp_pipeline)

        assert len(wiretap.output_records) == 1
        output_records = [record.field for record in wiretap.output_records]
        expected_data = (f'{raw_data}')
        assert expected_data in output_records[0]['raw'].value
    finally:
        #Cleanup 1st pipeline
        if sdc_executor.get_pipeline_status(syslog_pipeline).response.json().get('status') == 'RUNNING':
           sdc_executor.stop_pipeline(syslog_pipeline)
        sdc_executor.remove_pipeline(syslog_pipeline)
        #Cleanup 2nd pipeline
        if sdc_executor.get_pipeline_status(udp_pipeline).response.json().get('status') == 'RUNNING':
           sdc_executor.stop_pipeline(udp_pipeline)
        sdc_executor.remove_pipeline(udp_pipeline)
