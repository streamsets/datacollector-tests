from string import Template


def _primary_key_specification_json(column_name, column_type, data_type, size, precision, scale, signed, currency):
    return f'''
        \"{column_name}\": {{
            \"type\": {column_type},
            \"datatype\": \"{data_type}\",
            \"size\": {size},
            \"precision\": {precision},
            \"scale\": {scale},
            \"signed\": {signed},
            \"currency\": {currency}
        }}
    '''

PRIMARY_KEY_NON_NUMERIC_METADATA_MYSQL = f'''{{
            {_primary_key_specification_json("my_boolean", -7, "BIT", 1, 1, 0, "false", "false")},
            {_primary_key_specification_json("my_date", 91, "DATE", 10, 10, 0, "false", "false")},
            {_primary_key_specification_json("my_datetime", 93, "TIMESTAMP", 19, 19, 0, "false", "false")},
            {_primary_key_specification_json("my_timestamp", 93, "TIMESTAMP", 19, 19, 0, "false", "false")},
            {_primary_key_specification_json("my_time", 92, "TIME", 10, 10, 0, "false", "false")},
            {_primary_key_specification_json("my_year", 91, "DATE", 4, 4, 0, "false", "false")},
            {_primary_key_specification_json("my_char", 1, "CHAR", 10, 10, 0, "false", "false")},
            {_primary_key_specification_json("my_varchar", 12, "VARCHAR", 32, 32, 0, "false", "false")},
            {_primary_key_specification_json("my_varchar2", 12, "VARCHAR", 64, 64, 0, "false", "false")},
            {_primary_key_specification_json("my_text", 12, "VARCHAR", 255, 255, 0, "false", "false")}
        }}'''

PRIMARY_KEY_NUMERIC_METADATA_MYSQL = f'''{{
            {_primary_key_specification_json("my_bit", -7, "BIT", 1, 1, 0, "false", "false")},
            {_primary_key_specification_json("my_tinyint", -6, "TINYINT", 3, 3, 0, "true", "false")},
            {_primary_key_specification_json("my_smallint", 5, "SMALLINT", 5, 5, 0, "true", "false")},
            {_primary_key_specification_json("my_int", 4, "INTEGER", 10, 10, 0, "true", "false")},
            {_primary_key_specification_json("my_bigint", -5, "BIGINT", 19, 19, 0, "true", "false")},
            {_primary_key_specification_json("my_decimal", 3, "DECIMAL", 10, 10, 5, "true", "false")},
            {_primary_key_specification_json("my_numeric", 3, "DECIMAL", 8, 8, 4, "true", "false")},
            {_primary_key_specification_json("my_float", 7, "REAL", 12, 12, 0, "true", "false")},
            {_primary_key_specification_json("my_double", 8, "DOUBLE", 22, 22, 0, "true", "false")}
        }}'''

PRIMARY_KEY_NON_NUMERIC_METADATA_SQLSERVER = f'''{{
            {_primary_key_specification_json("my_date", 91, "DATE", 10, 10, 0, "false", "false")},
            {_primary_key_specification_json("my_datetime", 93, "TIMESTAMP", 23, 23, 3, "false", "false")},
            {_primary_key_specification_json("my_time", 92, "TIME", 16, 16, 7, "false", "false")},
            {_primary_key_specification_json("my_char", 1, "CHAR", 10, 10, 0, "false", "false")},
            {_primary_key_specification_json("my_varchar", 12, "VARCHAR", 32, 32, 0, "false", "false")},
            {_primary_key_specification_json("my_nchar", -15, "NCHAR", 64, 64, 0, "false", "false")},
            {_primary_key_specification_json("my_nvarchar", -9, "NVARCHAR", 128, 128, 0, "false", "false")}
        }}'''

PRIMARY_KEY_NUMERIC_METADATA_SQLSERVER = f'''{{
            {_primary_key_specification_json("my_bit", -7, "BIT", 1, 1, 0, "false", "false")},
            {_primary_key_specification_json("my_tinyint", -6, "TINYINT", 3, 3, 0, "false", "false")},
            {_primary_key_specification_json("my_smallint", 5, "SMALLINT", 6, 5, 0, "true", "false")},
            {_primary_key_specification_json("my_int", 4, "INTEGER", 11, 10, 0, "true", "false")},
            {_primary_key_specification_json("my_bigint", -5, "BIGINT", 20, 19, 0, "true", "false")},
            {_primary_key_specification_json("my_decimal", 3, "DECIMAL", 12, 10, 5, "true", "false")},
            {_primary_key_specification_json("my_numeric", 2, "NUMERIC", 10, 8, 4, "true", "false")},
            {_primary_key_specification_json("my_float", 8, "DOUBLE", 22, 15, 0, "true", "false")},
            {_primary_key_specification_json("my_money", 3, "DECIMAL", 21, 19, 4, "true", "true")}
        }}'''

PRIMARY_KEY_NON_NUMERIC_METADATA_POSTGRESQL = f'''{{
            {_primary_key_specification_json("my_boolean", -7, "BIT", 1, 1, 0, "false", "false")},
            {_primary_key_specification_json("my_date", 91, "DATE", 13, 13, 0, "false", "false")},
            {_primary_key_specification_json("my_timestamp", 93, "TIMESTAMP", 29, 29, 6, "false", "false")},
            {_primary_key_specification_json("my_time", 92, "TIME", 15, 15, 6, "false", "false")},
            {_primary_key_specification_json("my_char", 1, "CHAR", 16, 16, 0, "false", "false")},
            {_primary_key_specification_json("my_varchar", 12, "VARCHAR", 32, 32, 0, "false", "false")}
        }}'''

PRIMARY_KEY_NUMERIC_METADATA_POSTGRESQL = f'''{{
            {_primary_key_specification_json("my_smallint", 5, "SMALLINT", 6, 5, 0, "true", "false")},
            {_primary_key_specification_json("my_integer", 4, "INTEGER", 11, 10, 0, "true", "false")},
            {_primary_key_specification_json("my_bigint", -5, "BIGINT", 20, 19, 0, "true", "false")},
            {_primary_key_specification_json("my_decimal", 2, "NUMERIC", 6, 5, 0, "true", "false")},
            {_primary_key_specification_json("my_numeric", 2, "NUMERIC", 11, 10, 0, "true", "false")},
            {_primary_key_specification_json("my_real", 7, "REAL", 15, 8, 8, "true", "false")}
        }}'''

PRIMARY_KEY_NON_NUMERIC_METADATA_ORACLE = f'''{{
            {_primary_key_specification_json("MY_CHAR", 1, "CHAR", 16, 16, 0, "true", "false")},
            {_primary_key_specification_json("MY_NCHAR", -15, "NCHAR", 32, 32, 0, "true", "false")},
            {_primary_key_specification_json("MY_VARCHAR2", 12, "VARCHAR", 16, 16, 0, "true", "false")},
            {_primary_key_specification_json("MY_NVARCHAR2", -9, "NVARCHAR", 32, 32, 0, "true", "false")},
            {_primary_key_specification_json("MY_DATE", 93, "TIMESTAMP", 7, 0, 0, "true", "false")},
            {_primary_key_specification_json("MY_TIMESTAMP", 93, "TIMESTAMP", 11, 0, 3, "true", "false")}
        }}'''

PRIMARY_KEY_NUMERIC_METADATA_ORACLE = f'''{{
            {_primary_key_specification_json("MY_NUMBER_1", 2, "NUMERIC", 39, 0, -127, "true", "true")},
            {_primary_key_specification_json("MY_NUMBER_2", 2, "NUMERIC", 21, 20, 0, "true", "true")},
            {_primary_key_specification_json("MY_NUMBER_3", 2, "NUMERIC", 21, 20, 0, "true", "true")},
            {_primary_key_specification_json("MY_NUMBER_4", 2, "NUMERIC", 32, 30, 10, "true", "true")}
        }}'''

id_metadata_mysql = _primary_key_specification_json("id", 4, "INTEGER", 10, 10, 0, "true", "false")
name_metadata_mysql = _primary_key_specification_json("name", 12, "VARCHAR", 32, 32, 0, "false", "false")
PRIMARY_KEY_MYSQL_TABLE = f'{{ {id_metadata_mysql}, {name_metadata_mysql} }}'

id_metadata_sqlserver = _primary_key_specification_json("id", 4, "INTEGER", 11, 10, 0, "true", "false")
name_metadata_sqlserver = _primary_key_specification_json("name", 12, "VARCHAR", 32, 32, 0, "false", "false")
PRIMARY_KEY_SQLSERVER_TABLE = f'{{ {id_metadata_sqlserver}, {name_metadata_sqlserver} }}'

id_metadata_postgresql = _primary_key_specification_json("id", 4, "INTEGER", 11, 10, 0, "true", "false")
name_metadata_postgresql = _primary_key_specification_json("name", 12, "VARCHAR", 32, 32, 0, "false", "false")
PRIMARY_KEY_POSTGRESQL_TABLE = f'{{ {id_metadata_postgresql}, {name_metadata_postgresql} }}'

id_metadata_oracle = _primary_key_specification_json("ID", 2, "NUMERIC", 39, 38, 0, "true", "true")
name_metadata_oracle = _primary_key_specification_json("NAME", 12, "VARCHAR", 32, 32, 0, "true", "false")
PRIMARY_KEY_ORACLE_TABLE = f'{{ {id_metadata_oracle}, {name_metadata_oracle} }}'

CREATE_MYSQL_NUMERIC = '''
        create table $TABLE(
            my_bit bit,
            my_tinyint tinyint,
            my_smallint smallint,
            my_int int,
            my_bigint bigint,
            my_decimal decimal(10, 5),
            my_numeric numeric(8, 4),
            my_float float,
            my_double double,
            primary key (
                my_bit,
                my_tinyint,
                my_smallint,
                my_int,
                my_bigint,
                my_decimal,
                my_numeric,
                my_float,
                my_double
            )
        )
    '''

CREATE_MYSQL_NON_NUMERIC = '''
        create table $TABLE(
            my_boolean boolean,
            my_date date,
            my_datetime datetime,
            my_timestamp timestamp,
            my_time time,
            my_year year,
            my_char char(10),
            my_varchar varchar(32),
            my_varchar2 varchar(64),
            my_text text(16),
            primary key (
                my_boolean,
                my_date,
                my_datetime,
                my_timestamp,
                my_time,
                my_year,
                my_char,
                my_varchar,
                my_varchar2,
                my_text(16)
            )
        )
'''

INSERT_MYSQL_NUMERIC = '''insert into $TABLE values (1, 1, 1, 1, 1, 1.1, 1, 1.1, 1.1)'''
INSERT_MYSQL_NON_NUMERIC = '''insert into $TABLE
            values (true, '2011-12-18', '2011-12-18 9:17:17', current_timestamp, current_time, 2022, ' ', ' ', ' ', ' ')
        '''

CREATE_SQLSERVER_NUMERIC = '''
        create table $TABLE(
            my_bit bit,
            my_tinyint tinyint,
            my_smallint smallint,
            my_int int,
            my_bigint bigint,
            my_decimal decimal(10, 5),
            my_numeric numeric(8, 4),
            my_float float,
            my_money money,
            primary key (
                my_bit,
                my_tinyint,
                my_smallint,
                my_int,
                my_bigint,
                my_decimal,
                my_numeric,
                my_float,
                my_money
            )
        )
    '''

CREATE_SQLSERVER_NON_NUMERIC = '''
        create table $TABLE(
            my_date date,
            my_datetime datetime,
            my_time time,
            my_char char(10),
            my_varchar varchar(32),
            my_nchar nchar(64),
            my_nvarchar nvarchar(128),
            primary key (
                my_date,
                my_datetime,
                my_time,
                my_char,
                my_varchar,
                my_nchar,
                my_nvarchar
            )
        )
'''

INSERT_SQLSERVER_NUMERIC = '''insert into $TABLE values (1, 1, 1, 1, 1, 1.1, 1, 1.1, 1)'''
INSERT_SQLSERVER_NON_NUMERIC = '''insert into $TABLE
            values ('1993-12-10', '1993-12-10 1:23:45', '12:34', ' ', ' ', ' ', ' ')
        '''


CREATE_POSTGRESQL_NUMERIC = '''
        create table $TABLE(
            my_smallint smallint,
            my_integer integer,
            my_bigint bigint,
            my_decimal decimal(5),
            my_numeric numeric(10),
            my_real real,
            primary key (
                my_smallint,
                my_integer,
                my_bigint,
                my_decimal,
                my_numeric,
                my_real
            )
        )
    '''

CREATE_POSTGRESQL_NON_NUMERIC = '''
        create table $TABLE(
            my_char char(16),
            my_varchar varchar(32),
            my_date date,
            my_time time,
            my_timestamp timestamp,
            my_boolean boolean,
            primary key (
                my_char,
                my_varchar,
                my_date,
                my_time,
                my_timestamp,
                my_boolean
            )
        )
'''

INSERT_POSTGRESQL_NUMERIC = '''insert into $TABLE values (1, 1, 1, 1, 1, 1)'''
INSERT_POSTGRESQL_NON_NUMERIC = '''insert into $TABLE
            values (' ', ' ', '23-NOV-04', '12:34', current_timestamp, True)
        '''


CREATE_ORACLE_NUMERIC = '''
        create table $TABLE(
            my_number_1 number,
            my_number_2 number(20,0),
            my_number_3 number(20),
            my_number_4 number(30,10),
            primary key (
                my_number_1,
                my_number_2,
                my_number_3,
                my_number_4
            )
        )
    '''

CREATE_ORACLE_NON_NUMERIC = '''
        create table $TABLE(
            my_varchar2 VARCHAR2(16),
            my_nvarchar2 NVARCHAR2(32),
            my_char CHAR(16),
            my_nchar NCHAR(32),
            my_date date,
            my_timestamp timestamp(3),
            primary key (
                my_varchar2,
                my_nvarchar2,
                my_char,
                my_nchar,
                my_date,
                my_timestamp
            )
        )
'''

INSERT_ORACLE_NUMERIC = '''insert into $TABLE values (1, 1, 1, 1)'''
INSERT_ORACLE_NON_NUMERIC = '''insert into $TABLE
            values (' ', ' ', ' ', ' ', '23-NOV-04', current_timestamp)
        '''


CREATE_MYSQL_NUMERIC_TEMPLATE = Template(CREATE_MYSQL_NUMERIC)
CREATE_MYSQL_NON_NUMERIC_TEMPLATE = Template(CREATE_MYSQL_NON_NUMERIC)
INSERT_MYSQL_NUMERIC_TEMPLATE = Template(INSERT_MYSQL_NUMERIC)
INSERT_MYSQL_NON_NUMERIC_TEMPLATE = Template(INSERT_MYSQL_NON_NUMERIC)

CREATE_SQLSERVER_NUMERIC_TEMPLATE = Template(CREATE_SQLSERVER_NUMERIC)
CREATE_SQLSERVER_NON_NUMERIC_TEMPLATE = Template(CREATE_SQLSERVER_NON_NUMERIC)
INSERT_SQLSERVER_NUMERIC_TEMPLATE = Template(INSERT_SQLSERVER_NUMERIC)
INSERT_SQLSERVER_NON_NUMERIC_TEMPLATE = Template(INSERT_SQLSERVER_NON_NUMERIC)

CREATE_POSTGRESQL_NUMERIC_TEMPLATE = Template(CREATE_POSTGRESQL_NUMERIC)
CREATE_POSTGRESQL_NON_NUMERIC_TEMPLATE = Template(CREATE_POSTGRESQL_NON_NUMERIC)
INSERT_POSTGRESQL_NUMERIC_TEMPLATE = Template(INSERT_POSTGRESQL_NUMERIC)
INSERT_POSTGRESQL_NON_NUMERIC_TEMPLATE = Template(INSERT_POSTGRESQL_NON_NUMERIC)

CREATE_ORACLE_NUMERIC_TEMPLATE = Template(CREATE_ORACLE_NUMERIC)
CREATE_ORACLE_NON_NUMERIC_TEMPLATE = Template(CREATE_ORACLE_NON_NUMERIC)
INSERT_ORACLE_NUMERIC_TEMPLATE = Template(INSERT_ORACLE_NUMERIC)
INSERT_ORACLE_NON_NUMERIC_TEMPLATE = Template(INSERT_ORACLE_NON_NUMERIC)


def get_create_table_query_numeric(table, database):
    if database.type is 'Oracle':
        query = CREATE_ORACLE_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    elif database.type is 'SQLServer':
        query = CREATE_SQLSERVER_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    elif database.type is 'PostgreSQL':
        query = CREATE_POSTGRESQL_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    else:
        query = CREATE_MYSQL_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})

    return query

def get_create_table_query_non_numeric(table, database):
    if database.type is 'Oracle':
        query = CREATE_ORACLE_NON_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    elif database.type is 'SQLServer':
        query = CREATE_SQLSERVER_NON_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    elif database.type is 'PostgreSQL':
        query = CREATE_POSTGRESQL_NON_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    else:
        query = CREATE_MYSQL_NON_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})

    return query

def get_insert_query_numeric(table, database):
    if database.type is 'Oracle':
        query = INSERT_ORACLE_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    elif database.type is 'SQLServer':
        query = INSERT_SQLSERVER_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    elif database.type is 'PostgreSQL':
        query = INSERT_POSTGRESQL_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    else:
        query = INSERT_MYSQL_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})

    return query

def get_insert_query_non_numeric(table, database):
    if database.type is 'Oracle':
        query = INSERT_ORACLE_NON_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    elif database.type is 'SQLServer':
        query = INSERT_SQLSERVER_NON_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    elif database.type is 'PostgreSQL':
        query = INSERT_POSTGRESQL_NON_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})
    else:
        query = INSERT_MYSQL_NON_NUMERIC_TEMPLATE.safe_substitute({'TABLE': table})

    return query