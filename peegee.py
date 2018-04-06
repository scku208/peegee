# -*- coding: utf-8 -*-
import os
import sys
import psycopg2
import psycopg2.sql as psysql

VALID_EXTENSION_NAME = ('postgis',)
VALID_COLUMN_TYPE = (
    #Built-in general-purpose data types
    "bigint", "bigserial", "bit", "bit varying", "boolean", "box", "bytea",
    "character", "character varying", "cidr", "circle", "date",
    "double precision", "inet", "integer", "interval", "json", "jsonb",
    "line", "lseg", "macaddr", "macaddr8", "money", "numeric", "path",
    "pg_lsn", "point", "polygon", "real", "smallint", "smallserial", "serial",
    "text", "time", "time without time zone", "time with time zone",
    "timestamp", "timestamp without time zone", "timestamp with time zone",
    "tsquery", "tsvector", "txid_snapshot", "uuid", "xml",
    #Aliases
    "int8", "serial8", "varbit", "bool", "char", "varchar",
    "float8", "int", "int4", "decimal", "float4", "int2",
    "serial2", "serial4", "timetz", "timestamptz",
    )
PGSQL_SYSTEM_COLUMNS =\
    ['oid', 'tableoid', 'xmin',
     'cmin', 'xmax', 'cmax', 'ctid']

class Manager(object):
    def __init__(self, host='localhost', database='postgres', port=5432,
        user='postgres', password='', show_info=True):

        self.conn_param = dict(
            host=host, database=database,
            user=user, password=password, port=port)
        self.conn = psycopg2.connect(**self.conn_param)
        self.cur = self.conn.cursor()
        self.show_info = show_info

    def addColumn(self, table, column, type_='text', schema=None, commit=True):
        if type_ not in VALID_COLUMN_TYPE:
            raise ValueError('Type "{t}" is not a valid type. '\
                'You can manually add it to "VALID_COLUMN_TYPE" variable '\
                'if your column type is not list there.'.format(t=type_))
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if self.isTableExists(table, schema):
                if not self.isColumnExists(table, column, schema):
                    #already checked type_ above,
                    #but still use it at your own risk!!
                    sql_str = r"ALTER TABLE {sn}.{tn} ADD COLUMN {cn} "+\
                    "{ct}".format(ct=type_)
                    query = psysql.SQL(sql_str).format(
                        sn=psysql.Identifier(schema),
                        tn=psysql.Identifier(table),
                        cn=psysql.Identifier(column))
                    self.execute(query)
                    if commit:
                        self.commit()
                else:
                    print('The column "{c}" in table "{t}" already exists, '\
                        'just skipped.'.format(c=column, t=table))
            else:
                raise ValueError(
                    'The table "{t}" is not exists.'.format(t=table))
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def addGeometryColumn(self, table, column='', srid=4326, type_='GEOMETRY',
        dim=2, schema=None, commit=True):

        if not schema:
            schema = self.getCurrentSchema()
        if not column:
            column = 'geom'
        if self.isColumnExists(table, column, schema):
            print('The column "{c}" in table "{t}" already exists, '\
                      'just skipped.'.format(c=column, t=table))
            return
        type_ = type_.upper()

        self.execute(
            psysql.SQL(
               '''
               SELECT AddGeometryColumn(
               %(sn)s, %(tn)s, %(cn)s, %(srid)s, %(gt)s, %(dim)s)
               '''),
            dict(
                sn=schema, tn=table, cn=column, srid=srid, gt=type_, dim=dim)
            )
        if commit:
            self.commit()

    def close(self):
        #wrap
        self.cur.close()
        self.conn.close()

    def commit(self):
        #wrap
        self.conn.commit()

    def createDatabase(self, database):
        if not self.isDatabaseExists(database):
            try:
                self.conn.set_isolation_level(0)
                self.execute(psysql.SQL('CREATE DATABASE {db_n}').format(
                    db_n=psysql.Identifier(database)))
            finally:
                self.conn.set_isolation_level(1)
        else:
            print('The database {db_n} already exists, '\
                  'creation is skipped.'.format(db_n=database))

    def createExtension(self, extension):
        if extension not in VALID_EXTENSION_NAME:
            raise ValueError('Extension "{t}" is not a valid extension. '\
                'You can manually add it to "VALID_EXTENSION_NAME" variable '\
                'if your extension is not list there.'.format(t=extension))
        if not self.isExtensionExists(extension):
            self.execute('CREATE EXTENSION {ext_n}'.format(ext_n=extension))
        else:
            print('The extension "{ext_n}" already exists, '\
                  'the creation is skipped.'.format(ext_n=extension))

    def createSchema(self, schema, commit=True):
        if not self.isSchemaExists(schema):
            self.execute(
                psysql.SQL('CREATE SCHEMA {sn}').format(
                    sn=psysql.Identifier(schema))
                )
            if commit:
                self.commit()
        else:
            print(u'The schema {sm} already exists, '\
                  'creation is skipped.'.format(sm=schema))

    def createTable(self, table, column_name, column_type, schema=None,
        if_exists='skip', commit=True):

        if not schema:
            schema = self.getCurrentSchema()
        if self.isTableExists(table, schema) and if_exists == 'skip':
            print('The table "{sm}.{tb}" alreadly exists, just skipped'.format(
                sm=schema, tb=table))
            return
        for idx in range(len(column_name)):
            if column_name[idx] in PGSQL_SYSTEM_COLUMNS:
                raise ValueError(
                    'column name "{tbn}" conflicts '\
                    'with postgresql system columns name, '\
                    'please rename it (e.g. maybe adding suffix "_"'.format(
                        tbn=column_name[idx]))
        if isinstance(column_type, str):
            column_type_list = [column_type] * len(column_name)
        elif isinstance(column_type, dict):
            column_type_list = ["text"] * len(column_name)
            for k,v in column_type.iteritems():
                if isinstance(k, str):
                    idx_k = column_name.index(k)
                    column_type_list[idx_k] = v
                else:
                    column_type_list[k] = v
        elif isinstance(column_type, list):
            column_type_list = column_type
        #check valid type
        for ct_i in column_type_list:
            if ct_i not in VALID_COLUMN_TYPE:
                raise ValueError('Type "{t}" is not a valid type. '\
                    'You can manually add it to "VALID_COLUMN_TYPE" variable '\
                    'if your column type is not list there.'.format(t=ct_i))
        column_name_placeholder = ['{}'] * len(column_name)
        column_def = ", ".join(map(lambda x: ' '.join(x),
                                   zip(column_name_placeholder,
                                       column_type_list)))
        commands = []
        if self.isTableExists(table, schema) and if_exists == 'drop':
            #drop table
            commands.append(psysql.SQL(
                'DROP TABLE {sm}.{tn}').format(
                    sm=psysql.Identifier(schema),
                    tn=psysql.Identifier(table)))
        cmd_str = r'CREATE TABLE {}.{} (' + '{cd})'.format(cd=column_def)
        commands.append(psysql.SQL(cmd_str).format(
            *(map(psysql.Identifier, [schema, table] + column_name))
            ))
        for cmd in commands:
            self.execute(cmd)
        if commit:
            self.commit()

    def createUser(self, user, commit=True):
        if not self.isUserExists(user):
            self.execute(psysql.SQL(
                'CREATE ROLE {un} LOGIN').format(un=psysql.Identifier(user)))
            if commit:
                self.commit()
        else:
            print('The user "{un}" already exists, '\
                  'creation is skipped.'.format(un=user))

    def execute(self, query, vars_=None):
        if self.show_info:
            print(self.cur.mogrify(query, vars_).decode('utf8'))
        self.cur.execute(query, vars=vars_)

    def getAllColumnsInTable(self, table, schema=None):
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if self.isTableExists(table, schema):
                self.execute('''
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %(sm)s AND 
                          table_name = %(tb)s''',
                    dict(sm=schema, tb=table))
                return self._getFetchResultAtColumn(0)
            else:
                raise ValueError(
                    'The table "{t}" is not exists.'.format(t=table))
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def getAllDatabases(self):
        self.execute(
            'SELECT datname FROM pg_database WHERE datistemplate = false')
        return self._getFetchResultAtColumn(0)

    def getAllExtensions(self):
        self.execute('SELECT extname FROM pg_extension')
        return self._getFetchResultAtColumn(0)

    def getAllSchemas(self):
        self.execute(
            '''
            SELECT schema_name
            FROM information_schema.schemata
            WHERE
                schema_name NOT IN (
                    'pg_catalog', 'information_schema')
            AND schema_name NOT LIKE 'pg_%'
            '''
            )
        return self._getFetchResultAtColumn(0)

    def getAllRoles(self):
        self.execute('SELECT rolname FROM pg_roles WHERE rolcanlogin = false')
        return self._getFetchResultAtColumn(0)

    def getAllTablesInSchema(self, schema=None):
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            self.execute('''
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = %(sm)s'''
                , dict(sm=schema))
            return self._getFetchResultAtColumn(0)
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def getAllUsers(self):
        self.execute('SELECT rolname FROM pg_roles WHERE rolcanlogin = true')
        return self._getFetchResultAtColumn(0)

    def getCurrentDatabase(self):
        self.execute('SELECT current_database()')
        return self._getFetchResultAtColumn(0)[0]

    def getCurrentSchema(self):
        self.execute('SELECT current_schema FROM current_schema()')
        return self._getFetchResultAtColumn(0)[0]

    def isColumnExists(self, table, column, schema=None):
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if self.isTableExists(table, schema=schema):
                if column in self.getAllColumnsInTable(table, schema):
                    return True
                else:
                    return False
            else:
                raise ValueError(
                    'The table "{t}" is not exists.'.format(t=table))
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def isDatabaseExists(self, database):
        if database in self.getAllDatabases():
            return True
        else:
            return False

    def isExtensionExists(self, extension):
        if extension in self.getAllExtensions():
            return True
        else:
            return False

    def isRoleExists(self, role):
        if role in self.getAllRoles():
            return True
        else:
            return False

    def isSchemaExists(self, schema):
        if schema in self.getAllSchemas():
            return True
        else:
            return False

    def isTableExists(self, table, schema=None):
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if table in self.getAllTablesInSchema(schema):
                return True
            else:
                return False
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def isUserExists(self, user):
        if user in self.getAllUsers():
            return True
        else:
            return False

    def switchDatabase(self, database):
        if self.isDatabaseExists(database):
            self.conn_param['database'] = database
            self.__init__(**self.conn_param, show_info=self.show_info)
        else:
            raise ValueError(
                'The database "{d}" is not exists.'.format(d=database))

    def _getFetchResultAtColumn(self, idx=0):
        res = self.cur.fetchall()
        return [i[idx] for i in res]
