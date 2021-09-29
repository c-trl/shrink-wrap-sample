import psycopg2
import pymysql
import pandas as pd
from mapytools import credentials

class databases:
    
    def __init__(self,account='analytics',protocol=None,name=None,default_database=None):
        cred = credentials.get(account=account,protocol=protocol,name=name)
        self.host = cred['host']
        self.password = cred['password']
        self.port = cred['port']
        self.user = cred['user']
        if default_database is None:
            self.database = cred['database']
        else:
            self.database = default_database
        self.protocol = protocol

        if self.protocol == 'psql':
            self.con = psycopg2.connect(host=self.host,
                                       user=self.user,
                                       password=self.password,
                                       port=5432,
                                       database=self.database)
        elif self.protocol in ('mysql', 'aurora'):
            self.con = pymysql.connect(host=self.host,
                                       user=self.user,
                                       password=self.password,
                                       port=3306,
                                       database=self.database,
                                       autocommit=True)
        else:
            print('Panic now.')
    
    def database_query(self,query=None):
        cur = self.con.cursor()
        cur.execute(query)
        headers = [i[0] for i in cur.description]
        resp = cur.fetchall()
        return pd.DataFrame(list(resp),columns=headers)

    def database_command(self,command=None,query_check=None):
        cur = self.con.cursor()
        cur.execute(command)
        if query_check:
            cur.execute(query_check)
            headers = [i[0] for i in cur.description]
            resp = cur.fetchall()
            return pd.DataFrame(list(resp),columns=headers)
    
    def build_call(self,query=None,query_template=None,replacements=None,delimiter=','):
        
        self.delimiter = delimiter
        
        if query_template:
            with open(query_template, 'r') as f:
                self.query = f.read()
        elif not query_template and query:
            self.query = query
        else:
            raise Exception('Query string or template must be provided when initializing qubole_api')
        
        if replacements:
            for k in replacements:
                self.query = self.query.replace(k, str(replacements[k]))
    
    def make_call(self,save_as=None,df=False):
        try:
            df_tmp = self.database_query(self.query)
        except InternalError as err:
            self.con.rollback()
            raise err
        if df:
            self.df = df_tmp
        
        if save_as:
            df_tmp.to_csv(save_as, sep=self.delimiter, index=False)

    @property
    def databases(self):
        if self.protocol == 'psql':
            databases_query = 'select datname from pg_database where datistemplate = false'
        else:
            databases_query = 'show databases'
        return self.database_query(databases_query)
    
    def get_tables(self,database=None):
        if self.protocol == 'psql':
            if (database == self.database) or (database is None):
                tables_query = "select table_name from information_schema.tables where table_type = 'BASE TABLE' and table_schema = 'public' order by table_name"
                print('This will only get tables for the default database.')
            else:
                raise Exception('PostgreSQL can only query for tables within the default/specified database!')
        else:
            if not database:
                raise Exception('Must specify a database!')
            tables_query = 'show tables in {}'
        return self.database_query(tables_query.format(database))
    
    def get_table_schema(self,database=None,table=None):
        if database:
            database = database + '.'
        else:
            database = ''
        
        if not table:
            raise Exception('Must specify a table!')
        
        if self.protocol == 'psql':
            table_schema_query = "select column_name, udt_name, character_maximum_length from information_schema.columns where table_name = '{}' order by ordinal_position"
        else:
            table_schema_query = 'desc '+database+table
        
        return self.database_query(table_schema_query.format(table))

