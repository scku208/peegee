# -*- coding: utf-8 -*-
import os
import sys
import psycopg2
import psycopg2.sql as psysql

__version__ = '0.2.1'

TYPE_TO_IDENTIFIER = {
    'bigint': 'int8',
        'int8': 'int8',
    'bigserial': 'serial8',
        'serial8': 'serial8',
    'bit': 'bit',
    'bit varying': 'varbit',
        'varbit': 'varbit',
    'boolean': 'bool',
        'bool': 'bool',
    'box': 'box',
    'bytea': 'bytea',
    'character': 'char',
        'char': 'char',
    'character varying': 'varchar',
        'varchar': 'varchar',
    'cidr': 'cidr',
    'circle': 'circle',
    'date': 'date',
    'double precision': 'float8',
        'float8': 'float8',
    'inet': 'inet',
    'integer': 'int4',
        'int': 'int4',
        'int4': 'int4',
    'interval': 'interval',
    'json': 'json',
    'jsonb': 'jsonb',
    'line': 'line',
    'lseg': 'lseg',
    'macaddr': 'macaddr',
    'macaddr8': 'macaddr8',
    'money': 'money',
    'numeric': 'decimal',
        'decimal': 'decimal',
    'path': 'path',
    'pg_lsn': 'pg_lsn',
    'point': 'point',
    'polygon': 'polygon',
    'real': 'float4',
        'float4': 'float4',
    'smallint': 'int2',
        'int2': 'int2',
    'smallserial': 'serial2',
        'serial2': 'serial2',
    'serial': 'serial4',
        'serial4': 'serial4',
    'text': 'text',
    'time': 'time',
    'time with time zone': 'timetz',
        'timetz': 'timetz',
    'timestamp': 'timestamp',
    'timestamp with time zone': 'timestamptz',
        'timestamptz': 'timestamptz',
    'tsquery': 'tsquery',
    'tsvector': 'tsvector',
    'txid_snapshot': 'txid_snapshot',
    'uuid': 'uuid',
    'xml': 'xml',
    }

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
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if self.isTableExists(table, schema):
                if not self.isColumnExists(table, column, schema):
                    type_ = TYPE_TO_IDENTIFIER.get(type_.lower(), type_)
                    self.execute(psysql.SQL(
                        "ALTER TABLE {sn}.{tn} ADD COLUMN {cn} {ct}").format(
                            sn=psysql.Identifier(schema),
                            tn=psysql.Identifier(table),
                            cn=psysql.Identifier(column),
                            ct=psysql.Identifier(type_))
                        )
                    if commit:
                        self.commit()
                else:
                    raise ValueError(
                        'The column "{c}" in table "{t}" '\
                        'already exists.'.format(c=column, t=table))
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

    def createExtension(self, extension, schema=None):
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if not self.isExtensionExists(extension):
                self.execute(
                    psysql.SQL('CREATE EXTENSION {e} SCHEMA {s}').format(
                        e=psysql.Identifier(extension),
                        s=psysql.Identifier(schema)))
            else:
                if schema != self.getExtensionSchema(extension):
                    print('The extension "{e}" already exists in schema "{s}", '\
                      'move it to the specified schema "{s_to}".'.format(
                        e=extension,
                        s=self.getExtensionSchema(extension),
                        s_to=schema))
                    self.switchExtensionSchema(extension, schema)
                else:
                    print('The extension "{e}" already exists in schema "{s}", '\
                          'the creation is skipped.'.format(
                            e=extension,
                            s=schema))
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def createRole(self, role, commit=True):
        if not self.isRoleExists(role):
            self.execute(psysql.SQL(
                'CREATE ROLE {un}').format(un=psysql.Identifier(role)))
            if commit:
                self.commit()
        else:
            print('The role "{un}" already exists, '\
                  'creation is skipped.'.format(un=role))

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
            for k,v in column_type.items():
                if isinstance(k, str):
                    idx_k = column_name.index(k)
                    column_type_list[idx_k] = v
                else:
                    column_type_list[k] = v
        elif isinstance(column_type, list):
            column_type_list = column_type
        #back to column_type
        column_type = map(
            lambda t: TYPE_TO_IDENTIFIER.get(t.lower(), t),
            column_type_list)
        del column_type_list
        if self.isTableExists(table, schema) and if_exists == 'drop':
            self.dropTable(table, schema, commit=False)
        self.execute(psysql.SQL(
            'CREATE TABLE {s}.{t} ({cd})').format(
                s=psysql.Identifier(schema),
                t=psysql.Identifier(table),
                cd=psysql.SQL(', ').join(
                    map(psysql.SQL(' ').join, 
                        zip(map(psysql.Identifier, column_name),
                            map(psysql.Identifier, column_type)))
                    )
                )
            )
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

    def dropTable(self, table, schema=None, commit=True):
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if self.isTableExists(table, schema):
                self.execute(
                    psysql.SQL('DROP TABLE {sn}.{tn}').format(
                        sn=psysql.Identifier(schema),
                        tn=psysql.Identifier(table))
                    )
                if commit:
                    self.commit()
            else:
                raise ValueError(
                    'The table "{t}" is not exists.'.format(t=table))
       
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

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

    def getExtensionSchema(self, extension):
        if self.isExtensionExists(extension):
            self.execute(
                psysql.SQL('''SELECT nspname FROM pg_namespace, pg_extension 
                    WHERE pg_namespace.oid = pg_extension.extnamespace
                    AND pg_extension.extname = %(e)s
                    '''),
                dict(e=extension))
            return self._getFetchResultAtColumn(0)[0]
        else:
            raise ValueError(
                'The extension "{e}" is not exists.'.format(e=extension))

    def getSearchPath(self):
        self.execute('SHOW search_path')
        path_str = self._getFetchResultAtColumn(0)[0]
        return list(
            map(lambda x: x.strip().replace('"',''),path_str.split(',')))

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

    def renameColumn(self, table, column, to_column, schema=None, commit=True):
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if self.isTableExists(table, schema):
                if self.isColumnExists(table, column, schema):
                    self.execute(psysql.SQL(
                        'ALTER TABLE {s}.{t} RENAME COLUMN {c} TO {nc}').format(
                            s=psysql.Identifier(schema),
                            t=psysql.Identifier(table),
                            c=psysql.Identifier(column),
                            nc=psysql.Identifier(to_column)))
                    if commit:
                        self.commit()
                else:
                    raise ValueError(
                        'The column "{c}" is not exists.'.format(c=column))
            else:
                raise ValueError(
                    'The table "{t}" is not exists.'.format(t=table))
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def renameTable(self, table, to_table, schema=None, commit=True):
        if not schema:
            schema = self.getCurrentSchema()
        if self.isSchemaExists(schema):
            if self.isTableExists(table, schema):
                self.execute(psysql.SQL(
                    'ALTER TABLE {s}.{t} RENAME TO {nt}').format(
                        s=psysql.Identifier(schema),
                        t=psysql.Identifier(table),
                        nt=psysql.Identifier(to_table)))
                if commit:
                    self.commit()
            else:
                raise ValueError(
                    'The table "{t}" is not exists.'.format(t=table))
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def rollback(self):
        #warp
        self.conn.rollback()

    def setCurrentSchema(self, schema):
        if self.isSchemaExists(schema):
            self.setSearchPath([schema])
        else:
            raise ValueError(
                'The schema "{s}" is not exists.'.format(s=schema))

    def setSearchPath(self, search_path):
        self.execute(psysql.SQL('SET search_path TO {sp}').format(
            sp=psysql.SQL(', ').join(map(psysql.Identifier, search_path))))

    def switchDatabase(self, database):
        if self.isDatabaseExists(database):
            self.conn_param['database'] = database
            self.close()
            self.__init__(show_info=self.show_info, **self.conn_param)
        else:
            raise ValueError(
                'The database "{d}" is not exists.'.format(d=database))

    def switchExtensionSchema(self, extension, schema, commit=True):
        if self.isExtensionExists(extension):
            if self.isSchemaExists(schema):
                if self.getExtensionSchema != schema:
                    self.execute(
                        psysql.SQL('ALTER EXTENSION {e} SET SCHEMA {s}').format(
                            e=psysql.Identifier(extension),
                            s=psysql.Identifier(schema)))
                    if commit:
                        self.commit()
                else:
                    print('The extension "{e}" is already in schema "{s}"'\
                        'just skipped.'.format(e=extension, s=schema)) 
            else:
                raise ValueError(
                    'The schema "{s}" is not exists.'.format(s=schema)) 
        else:
            raise ValueError(
                'The extension "{e}" is not exists.'.format(e=extension))

    def _getFetchResultAtColumn(self, idx=0):
        res = self.cur.fetchall()
        return [i[idx] for i in res]
