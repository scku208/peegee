# peegee
A pythonic PostgreSQL client based on psycopg2 suit only for Python3

# Installation
    pip install peegee

# Basic usage
    import peegee as pg
    pgm = pg.Manager(host='localhost', database='postgres', port=5432,
        user='postgres', password='yourdatabaseloginpassword')
    
    # about get info
    pgm.getCurrentDatabase() # where am I -> 'postgres'
    pgm.getCurrentSchema() # think as default schema -> 'public'
    pgm.getAllDatabases() # list of database name, e.g. ['postgres', 'other_db', ...]
    pgm.getAllSchemas() # ['public', 'you_create_schema1', ...]
    pgm.getAllTablesInSchema() # return a list of all table name in "default" schema ('public')
    pgm.getAllTablesInSchema('another_schema') # specify schema name
    
    # about create
    pgm.createSchema('schema_name') # commit immediately by default
    pgm.createTable('table_name', column_name=['C1', 'C2', 'C3'],
        column_type='text', schema=None, if_exists='skip', commit=True)
        # create table named 'table_name' in default schema, 
        # has columns 'C1', 'C2', 'C3' with all data type "text"
        # if table already exists, just skip, then commit it!!
    
    # about check, return True or False
    pgm.isDatabaseExists('db_name')
    pgm.isSchemaExists('schema_name')
    pgm.isTableExists('table_name', schema=None) #default schema
    
    # about others
    pgm.switchDatabase('db_name') #link to other database
    pgm.createDatabase('new_db_name') #create db~
    
# You Should Note
The method of peegee's Manager, which including "assign column data type" or "create extension", that psycopg2 does not provide safety method, may get SQL injection (e.g. addColumn, createTable...etc.). I am trying to avoid SQL injection by adding "VALID_COLUMN_TYPE" and "VALID_EXTENSION_NAME" variables to verify input. But still, YOU SHOULD USE THESE METHOD WITH TRUST INPUT!

On the other hand, if your input should be trust, but is block by peegee, you can manually edit peegee.py by adding input to these variables, e.g. if "blah" extension can be trust and you want to create, just edit VALID_EXTENSION_NAME to ('postgis', 'blah'). then do:

    pgm.createExtension('blah') # should pass now
