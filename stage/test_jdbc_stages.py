import logging
import string

import pytest
import sqlalchemy

from testframework import environment, sdc
from testframework.utils import get_random_string

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture(scope='module')
def data_collector(args, database):
    data_collector = sdc.DataCollector(version=args.sdc_version)
    data_collector.add_stage_lib(*database.sdc_stage_libs)
    data_collector.start()
    yield data_collector
    if data_collector.tear_down_on_exit:
        data_collector.tear_down()

@pytest.fixture(scope='module')
def database(args):
    return environment.Database(database=args.database)

def test_jdbc_multitable_consumer_origin_simple(data_collector, database):
    pipeline_builder = data_collector.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    trash = pipeline_builder.add_stage('Trash')
    discard = pipeline_builder.add_error_stage('Discard')

    jdbc_multitable_consumer > trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(string.ascii_letters, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    rows_in_database = [
        {'name': 'Dima'},
        {'name': 'Jarcec'},
        {'name': 'Arvind'}
    ]

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), rows_in_database)

        data_collector.add_pipeline(pipeline)
        snapshot = data_collector.capture_snapshot(
            pipeline=pipeline,
            start_pipeline=True
        ).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)

        rows_from_snapshot = [{record.value['value'][1]['sqpath'].lstrip('/'):
                                   record.value['value'][1]['value']}
                              for record in snapshot[pipeline[0].instance_name].output]

        assert rows_from_snapshot == rows_in_database
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
