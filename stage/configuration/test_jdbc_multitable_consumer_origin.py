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
import logging
import string

import pytest
import sqlalchemy
from sqlalchemy import Column, Integer, String, CHAR
from streamsets.sdk.exceptions import ValidationError
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__file__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Manish'},
    {'id': 2, 'name': 'Shravan'},
    {'id': 3, 'name': 'Shubham'}
]

@database('postgresql')
@pytest.mark.parametrize('postgres_target_server', ['master', 'slave'])
def test_jdbc_multitable_consumer_origin_configuration_additional_jdbc_configuration_properties(sdc_builder,
                                                                                                sdc_executor,
                                                                                                database,
                                                                                                postgres_target_server):
    """Here we are testing disableColumnSanitiser and targetServerType parameter. Setting it to false should convert
    columns in result set to lower case. targetServerType - Should connect successfully if server is of
    targetServerType is master. The master/slave distinction is currently done by observing if the server allows
    writes. If targetServerTypeis slave it should raise error as server we are connecting to allows writes
    i.e. its of type master.
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    try:
        columns = [Column('id', Integer, primary_key=True), Column('NAME', String(32))]
        properties = [{'key': 'disableColumnSanitiser', 'value': 'false'},
                      {'key': 'targetServerType', 'value': postgres_target_server}]
        rows_in_database = [{'id': row['id'], 'NAME': row['name']} for row in ROWS_IN_DATABASE]
        table = create_table(database, columns, table_name)
        insert_data_in_table(database, table, rows_in_database)

        #Build the pipeline
        attributes = {'table_configs': [{"tablePattern": f'%{src_table_prefix}%'}],
                      'additional_jdbc_configuration_properties': properties}
        wiretap, pipeline = get_jdbc_multitable_consumer_to_trash_pipeline(sdc_builder, database,
                                                                           attributes)

        #Execute pipeline and check result.
        sdc_executor.add_pipeline(pipeline)
        if postgres_target_server == 'slave':
            with pytest.raises(Exception):
                sdc_executor.start_pipeline().wait_for_status('FINISHED')
        else:
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)

            # Column names are converted to lower case since database table columns are in upper case.
            tuples_to_lower_name = lambda tup: (tup[0].lower(), tup[1])
            rows_from_wiretap = [tuples_to_lower_name(list(record.field.items())[1])
                                  for record in wiretap.output_records]
            assert rows_from_wiretap == [('name', row['NAME']) for row in rows_in_database]
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        delete_table([table], database)


@database
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('create_jdbc_header_attributes', [True, False])
def test_jdbc_multitable_consumer_origin_configuration_create_header_attributes(sdc_builder,
                                                                                sdc_executor,
                                                                                database,
                                                                                create_jdbc_header_attributes):
    """Here we are testing disable Create_JDBC_Headers_Attributes checkbox. Setting it to false shouldn't add headers
    tables' fields. And setting it to true, create the headers (jdbc.) and added to the fields.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        rows_in_database = [{'id': row['id'], 'NAME': row['name']} for row in ROWS_IN_DATABASE]
        columns = [Column('id', Integer, primary_key=True), Column('NAME', String(32))]
        table = create_table(database, columns, table_name)
        insert_data_in_table(database, table, rows_in_database)

        #Build the pipeline
        attributes = {'table_configs': [{"tablePattern": table_name}],
                      'create_jdbc_header_attributes': create_jdbc_header_attributes}
        wiretap, pipeline = get_jdbc_multitable_consumer_to_trash_pipeline(sdc_builder,
                                                                           database,
                                                                           attributes)
        #Execute pipeline and check result.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        fields_with_headers = list(wiretap.output_records[0].header.values.keys())

        if create_jdbc_header_attributes:
            assert 5 <= len(fields_with_headers)
            assert 'jdbc.' == fields_with_headers[0][:5]

        elif not create_jdbc_header_attributes:
            assert 0 == len(fields_with_headers)

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        delete_table([table], database)


@database
@pytest.mark.parametrize('quote_character', ['BACKTICK', 'DOUBLE_QUOTES', 'NONE', 'SQUARE_BRACKETS'])
def test_jdbc_multitable_consumer_origin_configuration_quote_character(sdc_builder, sdc_executor, quote_character, database):
    builder = sdc_builder.get_pipeline_builder()
    table_name = get_random_string(string.ascii_letters, 10)
    offset_name = get_random_string(string.ascii_letters, 10)

    origin = builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
    origin.max_batch_size_in_records = 10

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination
    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    origin.table_configs[0]["tablePattern"] = f'%{table_name}%'

    origin.set_attributes(quote_character=quote_character)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(offset_name, sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )


    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()

        connection.execute(table.insert(), [{offset_name: 1}])

        sdc_executor.add_pipeline(pipeline)

        # Check if this DB and quoting character combination is correct
        if (quote_character is 'BACKTICK' and database.type is 'SQLServer')\
                or (quote_character in ('DOUBLE_QUOTES', 'SQUARE_BRACKETS') and database.type is 'MySQL')\
                or (quote_character in ('BACKTICK', 'NONE', 'SQUARE_BRACKETS') and database.type is 'Oracle')\
                or (quote_character in ('BACKTICK', 'NONE', 'SQUARE_BRACKETS') and database.type is 'PostgreSQL'):
            # If the combination is not allowed, check that the correct error is thrown
            try:
                sdc_executor.validate_pipeline(pipeline)
                assert False, 'This line should not be reached'
            except ValidationError as error:
                assert error.issues['issueCount'] == 1
                assert 'JDBC_414' in error.issues['stageIssues']['JDBCMultitableConsumer_01'][0]['message']

        else :
            # If the combination is correct, execute normally and check results
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
            sdc_executor.stop_pipeline(pipeline)

            # There should be no errors reported
            history = sdc_executor.get_pipeline_history(pipeline)
            assert history.latest.metrics.counter('stage.JDBCMultitableConsumer_01.errorRecords.counter').count == 0
            assert history.latest.metrics.counter('stage.JDBCMultitableConsumer_01.stageErrors.counter').count == 0

            # And verify that we properly read that one record
            assert len(wiretap.output_records) == 1
            assert wiretap.output_records[0].field[offset_name] == 1

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@pytest.mark.parametrize('auto_commit', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_auto_commit(sdc_builder, sdc_executor, auto_commit):
    pass


@pytest.mark.parametrize('per_batch_strategy', ['SWITCH_TABLES'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_batches_from_result_set(sdc_builder, sdc_executor, per_batch_strategy):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_connection_health_test_query(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_connection_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('convert_timestamp_to_string', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_convert_timestamp_to_string(sdc_builder, sdc_executor, convert_timestamp_to_string):
    pass


@pytest.mark.parametrize('data_time_zone', ['Africa/Abidjan', 'Africa/Accra', 'Africa/Addis_Ababa', 'Africa/Algiers', 'Africa/Asmara', 'Africa/Asmera', 'Africa/Bamako', 'Africa/Bangui', 'Africa/Banjul', 'Africa/Bissau', 'Africa/Blantyre', 'Africa/Brazzaville', 'Africa/Bujumbura', 'Africa/Cairo', 'Africa/Casablanca', 'Africa/Ceuta', 'Africa/Conakry', 'Africa/Dakar', 'Africa/Dar_es_Salaam', 'Africa/Djibouti', 'Africa/Douala', 'Africa/El_Aaiun', 'Africa/Freetown', 'Africa/Gaborone', 'Africa/Harare', 'Africa/Johannesburg', 'Africa/Juba', 'Africa/Kampala', 'Africa/Khartoum', 'Africa/Kigali', 'Africa/Kinshasa', 'Africa/Lagos', 'Africa/Libreville', 'Africa/Lome', 'Africa/Luanda', 'Africa/Lubumbashi', 'Africa/Lusaka', 'Africa/Malabo', 'Africa/Maputo', 'Africa/Maseru', 'Africa/Mbabane', 'Africa/Mogadishu', 'Africa/Monrovia', 'Africa/Nairobi', 'Africa/Ndjamena', 'Africa/Niamey', 'Africa/Nouakchott', 'Africa/Ouagadougou', 'Africa/Porto-Novo', 'Africa/Sao_Tome', 'Africa/Timbuktu', 'Africa/Tripoli', 'Africa/Tunis', 'Africa/Windhoek', 'America/Adak', 'America/Anchorage', 'America/Anguilla', 'America/Antigua', 'America/Araguaina', 'America/Argentina/Buenos_Aires', 'America/Argentina/Catamarca', 'America/Argentina/ComodRivadavia', 'America/Argentina/Cordoba', 'America/Argentina/Jujuy', 'America/Argentina/La_Rioja', 'America/Argentina/Mendoza', 'America/Argentina/Rio_Gallegos', 'America/Argentina/Salta', 'America/Argentina/San_Juan', 'America/Argentina/San_Luis', 'America/Argentina/Tucuman', 'America/Argentina/Ushuaia', 'America/Aruba', 'America/Asuncion', 'America/Atikokan', 'America/Atka', 'America/Bahia', 'America/Bahia_Banderas', 'America/Barbados', 'America/Belem', 'America/Belize', 'America/Blanc-Sablon', 'America/Boa_Vista', 'America/Bogota', 'America/Boise', 'America/Buenos_Aires', 'America/Cambridge_Bay', 'America/Campo_Grande', 'America/Cancun', 'America/Caracas', 'America/Catamarca', 'America/Cayenne', 'America/Cayman', 'America/Chicago', 'America/Chihuahua', 'America/Coral_Harbour', 'America/Cordoba', 'America/Costa_Rica', 'America/Creston', 'America/Cuiaba', 'America/Curacao', 'America/Danmarkshavn', 'America/Dawson', 'America/Dawson_Creek', 'America/Denver', 'America/Detroit', 'America/Dominica', 'America/Edmonton', 'America/Eirunepe', 'America/El_Salvador', 'America/Ensenada', 'America/Fort_Nelson', 'America/Fort_Wayne', 'America/Fortaleza', 'America/Glace_Bay', 'America/Godthab', 'America/Goose_Bay', 'America/Grand_Turk', 'America/Grenada', 'America/Guadeloupe', 'America/Guatemala', 'America/Guayaquil', 'America/Guyana', 'America/Halifax', 'America/Havana', 'America/Hermosillo', 'America/Indiana/Indianapolis', 'America/Indiana/Knox', 'America/Indiana/Marengo', 'America/Indiana/Petersburg', 'America/Indiana/Tell_City', 'America/Indiana/Vevay', 'America/Indiana/Vincennes', 'America/Indiana/Winamac', 'America/Indianapolis', 'America/Inuvik', 'America/Iqaluit', 'America/Jamaica', 'America/Jujuy', 'America/Juneau', 'America/Kentucky/Louisville', 'America/Kentucky/Monticello', 'America/Knox_IN', 'America/Kralendijk', 'America/La_Paz', 'America/Lima', 'America/Los_Angeles', 'America/Louisville', 'America/Lower_Princes', 'America/Maceio', 'America/Managua', 'America/Manaus', 'America/Marigot', 'America/Martinique', 'America/Matamoros', 'America/Mazatlan', 'America/Mendoza', 'America/Menominee', 'America/Merida', 'America/Metlakatla', 'America/Mexico_City', 'America/Miquelon', 'America/Moncton', 'America/Monterrey', 'America/Montevideo', 'America/Montreal', 'America/Montserrat', 'America/Nassau', 'America/New_York', 'America/Nipigon', 'America/Nome', 'America/Noronha', 'America/North_Dakota/Beulah', 'America/North_Dakota/Center', 'America/North_Dakota/New_Salem', 'America/Ojinaga', 'America/Panama', 'America/Pangnirtung', 'America/Paramaribo', 'America/Phoenix', 'America/Port-au-Prince', 'America/Port_of_Spain', 'America/Porto_Acre', 'America/Porto_Velho', 'America/Puerto_Rico', 'America/Punta_Arenas', 'America/Rainy_River', 'America/Rankin_Inlet', 'America/Recife', 'America/Regina', 'America/Resolute', 'America/Rio_Branco', 'America/Rosario', 'America/Santa_Isabel', 'America/Santarem', 'America/Santiago', 'America/Santo_Domingo', 'America/Sao_Paulo', 'America/Scoresbysund', 'America/Shiprock', 'America/Sitka', 'America/St_Barthelemy', 'America/St_Johns', 'America/St_Kitts', 'America/St_Lucia', 'America/St_Thomas', 'America/St_Vincent', 'America/Swift_Current', 'America/Tegucigalpa', 'America/Thule', 'America/Thunder_Bay', 'America/Tijuana', 'America/Toronto', 'America/Tortola', 'America/Vancouver', 'America/Virgin', 'America/Whitehorse', 'America/Winnipeg', 'America/Yakutat', 'America/Yellowknife', 'Antarctica/Casey', 'Antarctica/Davis', 'Antarctica/DumontDUrville', 'Antarctica/Macquarie', 'Antarctica/Mawson', 'Antarctica/McMurdo', 'Antarctica/Palmer', 'Antarctica/Rothera', 'Antarctica/South_Pole', 'Antarctica/Syowa', 'Antarctica/Troll', 'Antarctica/Vostok', 'Arctic/Longyearbyen', 'Asia/Aden', 'Asia/Almaty', 'Asia/Amman', 'Asia/Anadyr', 'Asia/Aqtau', 'Asia/Aqtobe', 'Asia/Ashgabat', 'Asia/Ashkhabad', 'Asia/Atyrau', 'Asia/Baghdad', 'Asia/Bahrain', 'Asia/Baku', 'Asia/Bangkok', 'Asia/Barnaul', 'Asia/Beirut', 'Asia/Bishkek', 'Asia/Brunei', 'Asia/Calcutta', 'Asia/Chita', 'Asia/Choibalsan', 'Asia/Chongqing', 'Asia/Chungking', 'Asia/Colombo', 'Asia/Dacca', 'Asia/Damascus', 'Asia/Dhaka', 'Asia/Dili', 'Asia/Dubai', 'Asia/Dushanbe', 'Asia/Famagusta', 'Asia/Gaza', 'Asia/Harbin', 'Asia/Hebron', 'Asia/Ho_Chi_Minh', 'Asia/Hong_Kong', 'Asia/Hovd', 'Asia/Irkutsk', 'Asia/Istanbul', 'Asia/Jakarta', 'Asia/Jayapura', 'Asia/Jerusalem', 'Asia/Kabul', 'Asia/Kamchatka', 'Asia/Karachi', 'Asia/Kashgar', 'Asia/Kathmandu', 'Asia/Katmandu', 'Asia/Khandyga', 'Asia/Kolkata', 'Asia/Krasnoyarsk', 'Asia/Kuala_Lumpur', 'Asia/Kuching', 'Asia/Kuwait', 'Asia/Macao', 'Asia/Macau', 'Asia/Magadan', 'Asia/Makassar', 'Asia/Manila', 'Asia/Muscat', 'Asia/Nicosia', 'Asia/Novokuznetsk', 'Asia/Novosibirsk', 'Asia/Omsk', 'Asia/Oral', 'Asia/Phnom_Penh', 'Asia/Pontianak', 'Asia/Pyongyang', 'Asia/Qatar', 'Asia/Qyzylorda', 'Asia/Rangoon', 'Asia/Riyadh', 'Asia/Saigon', 'Asia/Sakhalin', 'Asia/Samarkand', 'Asia/Seoul', 'Asia/Shanghai', 'Asia/Singapore', 'Asia/Srednekolymsk', 'Asia/Taipei', 'Asia/Tashkent', 'Asia/Tbilisi', 'Asia/Tehran', 'Asia/Tel_Aviv', 'Asia/Thimbu', 'Asia/Thimphu', 'Asia/Tokyo', 'Asia/Tomsk', 'Asia/Ujung_Pandang', 'Asia/Ulaanbaatar', 'Asia/Ulan_Bator', 'Asia/Urumqi', 'Asia/Ust-Nera', 'Asia/Vientiane', 'Asia/Vladivostok', 'Asia/Yakutsk', 'Asia/Yangon', 'Asia/Yekaterinburg', 'Asia/Yerevan', 'Atlantic/Azores', 'Atlantic/Bermuda', 'Atlantic/Canary', 'Atlantic/Cape_Verde', 'Atlantic/Faeroe', 'Atlantic/Faroe', 'Atlantic/Jan_Mayen', 'Atlantic/Madeira', 'Atlantic/Reykjavik', 'Atlantic/South_Georgia', 'Atlantic/St_Helena', 'Atlantic/Stanley', 'Australia/ACT', 'Australia/Adelaide', 'Australia/Brisbane', 'Australia/Broken_Hill', 'Australia/Canberra', 'Australia/Currie', 'Australia/Darwin', 'Australia/Eucla', 'Australia/Hobart', 'Australia/LHI', 'Australia/Lindeman', 'Australia/Lord_Howe', 'Australia/Melbourne', 'Australia/NSW', 'Australia/North', 'Australia/Perth', 'Australia/Queensland', 'Australia/South', 'Australia/Sydney', 'Australia/Tasmania', 'Australia/Victoria', 'Australia/West', 'Australia/Yancowinna', 'Brazil/Acre', 'Brazil/DeNoronha', 'Brazil/East', 'Brazil/West', 'CET', 'CST6CDT', 'Canada/Atlantic', 'Canada/Central', 'Canada/Eastern', 'Canada/Mountain', 'Canada/Newfoundland', 'Canada/Pacific', 'Canada/Saskatchewan', 'Canada/Yukon', 'Chile/Continental', 'Chile/EasterIsland', 'Cuba', 'EET', 'EST5EDT', 'Egypt', 'Eire', 'Etc/Greenwich', 'Etc/UCT', 'Etc/UTC', 'Etc/Universal', 'Etc/Zulu', 'Europe/Amsterdam', 'Europe/Andorra', 'Europe/Astrakhan', 'Europe/Athens', 'Europe/Belfast', 'Europe/Belgrade', 'Europe/Berlin', 'Europe/Bratislava', 'Europe/Brussels', 'Europe/Bucharest', 'Europe/Budapest', 'Europe/Busingen', 'Europe/Chisinau', 'Europe/Copenhagen', 'Europe/Dublin', 'Europe/Gibraltar', 'Europe/Guernsey', 'Europe/Helsinki', 'Europe/Isle_of_Man', 'Europe/Istanbul', 'Europe/Jersey', 'Europe/Kaliningrad', 'Europe/Kiev', 'Europe/Kirov', 'Europe/Lisbon', 'Europe/Ljubljana', 'Europe/London', 'Europe/Luxembourg', 'Europe/Madrid', 'Europe/Malta', 'Europe/Mariehamn', 'Europe/Minsk', 'Europe/Monaco', 'Europe/Moscow', 'Europe/Nicosia', 'Europe/Oslo', 'Europe/Paris', 'Europe/Podgorica', 'Europe/Prague', 'Europe/Riga', 'Europe/Rome', 'Europe/Samara', 'Europe/San_Marino', 'Europe/Sarajevo', 'Europe/Saratov', 'Europe/Simferopol', 'Europe/Skopje', 'Europe/Sofia', 'Europe/Stockholm', 'Europe/Tallinn', 'Europe/Tirane', 'Europe/Tiraspol', 'Europe/Ulyanovsk', 'Europe/Uzhgorod', 'Europe/Vaduz', 'Europe/Vatican', 'Europe/Vienna', 'Europe/Vilnius', 'Europe/Volgograd', 'Europe/Warsaw', 'Europe/Zagreb', 'Europe/Zaporozhye', 'Europe/Zurich', 'GB', 'GB-Eire', 'GMT', 'GMT0', 'Greenwich', 'Hongkong', 'Iceland', 'Indian/Antananarivo', 'Indian/Chagos', 'Indian/Christmas', 'Indian/Cocos', 'Indian/Comoro', 'Indian/Kerguelen', 'Indian/Mahe', 'Indian/Maldives', 'Indian/Mauritius', 'Indian/Mayotte', 'Indian/Reunion', 'Iran', 'Israel', 'Jamaica', 'Japan', 'Kwajalein', 'Libya', 'MET', 'MST7MDT', 'Mexico/BajaNorte', 'Mexico/BajaSur', 'Mexico/General', 'NZ', 'NZ-CHAT', 'Navajo', 'PRC', 'PST8PDT', 'Pacific/Apia', 'Pacific/Auckland', 'Pacific/Bougainville', 'Pacific/Chatham', 'Pacific/Chuuk', 'Pacific/Easter', 'Pacific/Efate', 'Pacific/Enderbury', 'Pacific/Fakaofo', 'Pacific/Fiji', 'Pacific/Funafuti', 'Pacific/Galapagos', 'Pacific/Gambier', 'Pacific/Guadalcanal', 'Pacific/Guam', 'Pacific/Honolulu', 'Pacific/Johnston', 'Pacific/Kiritimati', 'Pacific/Kosrae', 'Pacific/Kwajalein', 'Pacific/Majuro', 'Pacific/Marquesas', 'Pacific/Midway', 'Pacific/Nauru', 'Pacific/Niue', 'Pacific/Norfolk', 'Pacific/Noumea', 'Pacific/Pago_Pago', 'Pacific/Palau', 'Pacific/Pitcairn', 'Pacific/Pohnpei', 'Pacific/Ponape', 'Pacific/Port_Moresby', 'Pacific/Rarotonga', 'Pacific/Saipan', 'Pacific/Samoa', 'Pacific/Tahiti', 'Pacific/Tarawa', 'Pacific/Tongatapu', 'Pacific/Truk', 'Pacific/Wake', 'Pacific/Wallis', 'Pacific/Yap', 'Poland', 'Portugal', 'ROK', 'Singapore', 'SystemV/AST4', 'SystemV/AST4ADT', 'SystemV/CST6', 'SystemV/CST6CDT', 'SystemV/EST5', 'SystemV/EST5EDT', 'SystemV/HST10', 'SystemV/MST7', 'SystemV/MST7MDT', 'SystemV/PST8', 'SystemV/PST8PDT', 'SystemV/YST9', 'SystemV/YST9YDT', 'Turkey', 'UCT', 'US/Alaska', 'US/Aleutian', 'US/Arizona', 'US/Central', 'US/East-Indiana', 'US/Eastern', 'US/Hawaii', 'US/Indiana-Starke', 'US/Michigan', 'US/Mountain', 'US/Pacific', 'US/Pacific-New', 'US/Samoa', 'UTC', 'Universal', 'W-SU', 'WET', 'Zulu'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_data_time_zone(sdc_builder, sdc_executor, data_time_zone):
    pass


@pytest.mark.parametrize('enforce_read_only_connection', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_enforce_read_only_connection(sdc_builder, sdc_executor, enforce_read_only_connection):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_fetch_size(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_idle_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_init_query(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('initial_table_order_strategy', ['ALPHABETICAL', 'NONE', 'REFERENTIAL_CONSTRAINTS'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_initial_table_order_strategy(sdc_builder, sdc_executor,
                                                                                    initial_table_order_strategy):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_jdbc_connection_string(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_jdbc_driver_class_name(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_max_blob_size_in_bytes(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_max_clob_size_in_characters(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_maximum_pool_size(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_minimum_idle_connections(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_new_table_discovery_interval(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_no_more_data_event_generation_delay_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_number_of_retries_on_sql_error(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_number_of_threads(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_on_record_error(sdc_builder, sdc_executor, on_record_error):
    pass


@pytest.mark.parametrize('on_unknown_type', ['CONVERT_TO_STRING', 'STOP_PIPELINE'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_on_unknown_type(sdc_builder, sdc_executor, on_unknown_type):
    pass


@pytest.mark.parametrize('use_credentials', [True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_password(sdc_builder, sdc_executor, use_credentials):
    pass


@pytest.mark.parametrize('per_batch_strategy', ['PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE', 'SWITCH_TABLES'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_per_batch_strategy(sdc_builder, sdc_executor, per_batch_strategy):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_queries_per_second(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('per_batch_strategy', ['SWITCH_TABLES'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_result_set_cache_size(sdc_builder, sdc_executor, per_batch_strategy):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_table_configs(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('transaction_isolation', ['DEFAULT', 'TRANSACTION_READ_COMMITTED', 'TRANSACTION_READ_UNCOMMITTED', 'TRANSACTION_REPEATABLE_READ', 'TRANSACTION_SERIALIZABLE'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_transaction_isolation(sdc_builder, sdc_executor, transaction_isolation):
    pass


@pytest.mark.parametrize('use_credentials', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_use_credentials(sdc_builder, sdc_executor, use_credentials):
    pass


@pytest.mark.parametrize('use_credentials', [True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_username(sdc_builder, sdc_executor, use_credentials):
    pass


# Util functions

def create_table(database, columns, table_name):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        *columns
    )
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


def get_jdbc_multitable_consumer_to_trash_pipeline(sdc_builder, database, attributes,
                                                   configure_for_environment_flag=True):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(**attributes)
    wiretap = pipeline_builder.add_wiretap()
    jdbc_multitable_consumer >> wiretap.destination
    if configure_for_environment_flag:
        pipeline = pipeline_builder.build().configure_for_environment(database)
    else:
        pipeline = pipeline_builder.build()
    return wiretap, pipeline


def insert_data_in_table(database, table, rows_to_insert):
    logger.info('Adding three rows into %s database ...', database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), rows_to_insert)


def delete_table(tables, database):
    for table in tables:
        logger.info('Dropping table %s in %s database...', table.name, database.type)
        table.drop(database.engine)
