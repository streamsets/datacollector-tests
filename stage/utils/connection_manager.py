# Copyright 2024 StreamSets Inc.
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

from typing import Callable

from sqlalchemy.sql.expression import asc
from sqlalchemy.engine import Engine
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    String,
    Integer,
    and_,
    text
)
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


class SqlConnectionManager:
    def __init__(self, engine: Engine, cleanup: Callable):
        self.engine = engine
        self.cleanup = cleanup
        self.cleanup(engine.dispose)

    def execute_query(self, query: str) -> any:
        """Executes the given query."""
        return self.engine.execute(query)

    def create_table(self, table: Table = None, columns: tuple = None) -> Table:
        """Creates a table and returns it. If there is no Table passed, a default table is created."""
        if table is None:
            table = self.describe_table(columns=columns)
        logger.info(f'Creating Table {table.name}')
        table.create(self.engine)
        return table

    def describe_table(self, table_name: str = None, columns: tuple = None) -> Table:
        """Describes a table definition and returns it."""
        if table_name is None:
            table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}'
        if columns is None:
            columns = ({Column('id', Integer, primary_key=True), Column('name', String(32))})
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            *columns
        )
        logger.info(f'Describing Table {table.name}')
        self.cleanup(table.drop, self.engine)
        return table

    def populate_table(self, table: Table) -> Table:
        """
            It will get the table definition from the database, in case it has changed.
        """
        metadata = MetaData()
        return Table(table.name, metadata, autoload=True, autoload_with=self.engine)

    def insert(self, table: Table, records: list):
        """
            Inserts the given records.
        """
        insert_stmt = table.insert()
        return self.engine.execute(insert_stmt, records)

    def update(self, table: Table, records: list, where_clauses: list):
        """
            Update the given records.
        """
        update_stmt = table.update().where(and_(text(where_clause) for where_clause in where_clauses))
        return self.engine.execute(update_stmt, records)

    def delete(self, table: Table, records: list, where_clauses: list):
        """
            Update the given records.
        """
        delete_stmt = table.delete().where(and_(text(where_clause) for where_clause in where_clauses))
        return self.engine.execute(delete_stmt, records)

    def column_names(self, table: Table) -> list:
        """
            Gets the list of column names
        """
        return [column.name for column in table.columns]

    def column_definitions(self, table: Table) -> list:
        """
            Gets the list of column types
        """
        return [column.type for column in table.columns]

    def primary_key_definition(self, table: Table) -> list:
        """
            Gets the list of primary key column names
        """
        return [column.name for column in table.primary_key]

    def select_from_table(self, table: Table, order_by_column: str = None) -> list:
        """
            Selects all data from a table and returns it with columns ordered by
            column name and values ordered by column value passed, or by first column values.
        """
        table_column_names = sorted(table.columns.keys())
        if order_by_column is None:
            order_by_column = table_column_names[0] or ""
        result = self.engine.execute(table.select().order_by(asc(order_by_column)))
        database_rows = result.fetchall()
        result.close()
        logger.info(f'Selected {len(database_rows)} rows from Table {table.name}')
        return self.generate_database_result(database_rows, table_column_names)

    def generate_database_result(self, rows: list, column_names: list) -> list:
        """
            Generates a [{column_name_1: column_value_1, ...}, ...] structure using the
            rows and column_names of a query result.
        """
        json_rows = []
        for row in rows:
            row_dict = {}
            for column_name in column_names:
                row_dict[column_name] = getattr(row, column_name)
            json_rows.append(row_dict)
        return json_rows


@pytest.fixture
def sql_connection_manager(database: any, cleanup: Callable) -> SqlConnectionManager:
    return SqlConnectionManager(database.engine, cleanup)
