import pytest
import json
import os
import tempfile

from streamsets.testframework.utils import get_random_string


@pytest.mark.parametrize('raw_data', (
  '"A\\u0001\\r\\n\\u000C B\\r\\n C"',  # string type
  '["A\\u0001\\r\\n\\u000C B\\r\\n C", "Second Item"]',  # list type
  '{ "content" : "A\\u0001\\r\\n\\u000C B\\r\\n C" }'  # map type
))
@pytest.mark.parametrize('target_field', ('result', 'text'))
def test_json_parser(sdc_builder, sdc_executor, raw_data, target_field):
    """Test JSON parser processor. We also test removal of ASCII control characters.
    The pipeline would look like:

        dev_raw_data_source >> json_parser >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    json_parser = pipeline_builder.add_stage('JSON Parser', type='processor')
    json_parser.set_attributes(field_to_parse='/text', ignore_control_characters=True, target_field=f'/{target_field}')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> json_parser >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # remove ASCII control characters in the expected result
        expected = json.loads(raw_data.encode('ascii', 'ignore').decode())
        assert expected == wiretap.output_records[0].field[target_field]
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_json_parser_input_error(sdc_builder, sdc_executor):
    """Test JSON parser processor with an invalid input value. The pipeline would look like:

        dev_raw_data_source >> json_parser >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='{ "A" }', stop_after_first_batch=True)
    json_parser = pipeline_builder.add_stage('JSON Parser', type='processor')
    json_parser.set_attributes(field_to_parse='/text', target_field='result')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> json_parser >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # JSONP_03 - Cannot parse the JSON field
        assert 'JSONP_03' == wiretap.error_records[0].header['errorCode']
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('test_data', [
    {'field_type': 'BOOLEAN', 'value': '{"json": true}', 'format': '', 'error_code': None},
    {'field_type': 'CHAR', 'value': '{"json": "A"}', 'format': '', 'error_code': 'JSONP_03'},
    {'field_type': 'BYTE', 'value': '{"json": 1}', 'format': '', 'error_code': None},
    {'field_type': 'SHORT', 'value': '{"json": 2}', 'format': '', 'error_code': None},
    {'field_type': 'INTEGER', 'value': '{"json": 3}', 'format': '', 'error_code': None},
    {'field_type': 'LONG', 'value': '{"json": 4}', 'format': '', 'error_code': None},
    {'field_type': 'FLOAT', 'value': '{"json": 5.6}', 'format': '', 'error_code': None},
    {'field_type': 'DOUBLE', 'value': '{"json": 7.8}', 'format': '', 'error_code': None},
    {'field_type': 'DECIMAL', 'value': '{"json": 9}', 'format': '', 'error_code': None},
    {'field_type': 'STRING', 'value': '{"json": "{invalid_json"}', 'format': '', 'error_code': 'JSONP_03'},
    {'field_type': 'DATE', 'value': '{"json": "2020-01-01"}', 'format': 'yyyy-MM-dd', 'error_code': 'JSONP_03'},
    {'field_type': 'TIME', 'value': '{"json": "00:00:00.000"}', 'format': 'HH:mm:ss.SSS', 'error_code': 'JSONP_03'},
    {'field_type': 'DATETIME', 'value': '{"json": "2020-01-01 00:00:00.000"}', 'format': 'yyyy-MM-dd HH:mm:ss.SSS', 'error_code': 'JSONP_03'},
    {'field_type': 'ZONED_DATETIME', 'value': '{"json": "2020-01-01 00:00:00.000Z"}', 'format': 'yyyy-MM-dd HH:mm:ss.SSSX', 'error_code': 'JSONP_03'},
    {'field_type': 'BYTE_ARRAY', 'value': '{"json": "{}"}', 'format': '', 'error_code': 'API_03'},
    {'field_type': 'MAP', 'value': '{"json": {}}', 'format': None, 'error_code': 'API_16'},
    {'field_type': 'LIST', 'value': '{"json": []}', 'format': None, 'error_code': 'API_13'}
])
def test_field_types(sdc_builder, sdc_executor, test_data):
    """
    By definition the JSON parser processor expects a serialized JSON object in a string field.
    If a field is not a string we expect JSON parser to add an error record.

    The test pipeline is as follows:

    Dev Raw Data Source >> Field Type Converter >> JSON Parser >> Wiretap

    The field type converter is responsible to convert a field value into a field of a given type.
    For LIST and MAP fields the conversion is not needed and the Field Type Converter processor is omitted.
    If the target field type doesn't support conversion to a string we expect an API_XXX error.
    If the conversion to a string is possible we expect a JSONP_XXX error when a string contains
    an invalid JSON object (e.g. date-time fields) or no errors when a value is a valid json (e.g. boolean, number)
    """

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.raw_data = test_data['value']
    source.data_format = 'JSON'
    source.stop_after_first_batch = True

    converter = None
    if test_data['format'] is not None:
        converter = builder.add_stage('Field Type Converter', type='processor')
        converter.conversion_method = 'BY_FIELD'
        converter.field_type_converter_configs = [{
            "fields": ['/json'],
            "targetType": test_data['field_type'],
            "treatInputFieldAsDate": False,
            "dataLocale": "en,US",
            "scale": -1,
            "decimalScaleRoundingStrategy": "ROUND_UNNECESSARY",
            "dateFormat": "OTHER",
            "zonedDateTimeFormat": "OTHER",
            "encoding": "UTF-8",
            "otherDateFormat": test_data['format'],
            "otherZonedDateTimeFormat": test_data['format'],
            "zonedDateTimeTargetTimeZone": "UTC"
        }]

    parser = builder.add_stage('JSON Parser', type='processor')
    parser.field_to_parse = '/json'
    parser.target_field = '/json'

    wiretap = builder.add_wiretap()

    if converter:
        source >> converter >> parser >> wiretap.destination
    else:
        source >> parser >> wiretap.destination

    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_status(status='FINISHED')

        if test_data['error_code'] is None:
            assert len(wiretap.error_records) == 0
            assert len(wiretap.output_records) == 1

            obj = json.loads(test_data['value'])
            assert obj['json'] == wiretap.output_records[0].field['json']
        else:
            assert len(wiretap.output_records) == 0
            assert (len(wiretap.error_records) == 1
                    and test_data['error_code'] in wiretap.error_records[0].header['errorMessage'])
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_file_ref_field_type(sdc_builder, sdc_executor):
    """
    By definition the JSON parser processor expects a serialized JSON object in a string field.
    If a field is a file_ref field we expect JSON parser to add an error record.

    The test pipeline is as follows:

    Directory >> JSON Parser >> Wiretap
    """

    file_name = 'sdc.json'
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string())

    sdc_executor.execute_shell(f'mkdir -p {tmp_directory}')

    try:
        sdc_executor.write_file(os.path.join(tmp_directory, file_name), '{"json": "{}"}\n')

        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Directory')
        source.files_directory = tmp_directory
        source.file_name_pattern = file_name
        source.data_format = 'WHOLE_FILE'

        parser = builder.add_stage('JSON Parser', type='processor')
        parser.field_to_parse = '/fileRef'
        parser.target_field = '/json'

        wiretap = builder.add_wiretap()

        source >> parser >> wiretap.destination

        pipeline = builder.build()

        sdc_executor.add_pipeline(pipeline)

        status = sdc_executor.start_pipeline(pipeline)
        status.wait_for_pipeline_error_records_count(1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 0
        assert (len(wiretap.error_records) == 1
                and 'API_24' in wiretap.error_records[0].header['errorMessage'])

    finally:
        sdc_executor.execute_shell(f'rm -fr {tmp_directory}')
        sdc_executor.remove_pipeline(pipeline)

