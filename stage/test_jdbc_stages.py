import logging
import string

import sqlalchemy

from testframework.utils import get_random_string

logger = logging.getLogger(__name__)

def test_jdbc_multitable_consumer_origin_simple(sdc_builder, sdc_executor, database):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

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

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline)

        rows_from_snapshot = [{record.value['value'][1]['sqpath'].lstrip('/'):
                                   record.value['value'][1]['value']}
                              for record in snapshot[pipeline[0].instance_name].output]

        assert rows_from_snapshot == rows_in_database
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
