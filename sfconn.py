import snowflake.connector as sf
from snowflake.connector.errors import DatabaseError
from snowflake.connector.errors import ProgrammingError
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

class SfConn():

    def __init__(self, config, logger):
        
        self.user      = config['user']
        self.account   = config['account']
        self.role      = config['role']
        self.warehouse = config['warehouse']
        self.database  = config['database']
        self.schema    = config['schema']
        self.home      = os.environ['HOME']
        self.logger    = logger

        with open(os.path.join(self.home, '.snowflake', 'rsa_key.p8'), "rb") as key:
            p_key= serialization.load_pem_private_key(
                key.read(),
                password=None,
                backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
        self.logger.debug('Connecting to Snowflake..')
        try:
            self.conn = sf.connect(
                user=self.user,
                account=self.account,
                private_key=pkb,
                role=self.role,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )

        except sf.errors.ProgrammingError as e:
            # print(f"Progamming Error connecting: {e}")
            # sys.exit(2)
            raise sf.errors.ProgrammingError(f"Progamming Error connecting: {e}")
        except Exception as e:
            # print(f"Error connecting: {e}")
            # sys.exit(2)
            self.logger.error(f"Error Connecting: {e}")
            raise Exception(f"Error connecting: {e}")
        cursor = self.run_query("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")
        self.logger.debug('Connected to Snowflake')
        self.verify_conn()
        
    def verify_conn(self):
        curs = self.conn.cursor()
        self.logger.debug("Verifying connection ...")
        try:
            curs.execute("SELECT current_version(), current_role(), current_database(), current_schema(), current_warehouse()")
            one_row = curs.fetchone()
            self.logger.debug(f"Connected to {self.account}\nSnowflake version: {one_row[0]}\nRole: {one_row[1]}\nDB: {one_row[2]}\nSchema: {one_row[3]}\nWarehouse: {one_row[4]}")
            return True
        except Exception as e:
            self.logger.debug(f"Failed with: {e}")
            return False
        finally:
            curs.close()
        
    def close_conn(self):
        self.logger.debug("Disconnecting from Snowflake..")
        self.conn.close()

    # Run a query on a cursor. Cursor created if none passed in. Don't forget to close cursor upon use.
    def run_query(self, query , cursor = None):
        """ Runs a query using a cursor object. Cursor created if none passed.
        Args:
            conn(obj) - Snowflake connector object.
            query(str) - Query string to be run.
        Returns:
            Returns the cursor object upon success

        """
        if cursor:
            curs = cursor
        else:
            curs = self.conn.cursor()
        try:
            curs.execute(query)
        except sf.errors.ProgrammingError as e:
            #err_msg = f"DB Error when running query: '{query}': {e}"
            #print(err_msg)
            raise e
        return curs

    def run_multiple_queries(self, query, cursor = None):
        query_list = query.strip().split(';')
        print(query_list)
        if len(query_list) > 1:
            for query in query_list[:-1]:
                curs = self.run_query(self.conn, query, cursor)
        else:
            curs = self.run_query(self.conn, query, cursor)
        return curs
        
    def list_db_sc(self):
            done = 0
            db_sc = []
            query = """select catalog_name as database_name, schema_name from snowflake.account_usage.schemata
where deleted is null and schema_name not in ('INFORMATION_SCHEMA') and database_name not in ('SNOWFLAKE')
order by catalog_name, schema_name desc
"""         
            self.logger.debug("Fetching database.schema information from snowflake.account_usage.schemata")
            try:
                cursor = self.run_query(query)
                for row in cursor:
                    db_sc.append([ row[0], row[1] ])
                done = 1
            except ProgrammingError as e:
                self.logger.debug(f"Current role may not have access to snowflake.account_usage schema: {e}")
                pass
            # If current role does not have access to query snowflake.account_usage?
            if done == 0:
                self.logger.debug("Attemping to fetch database.schema through show database + show schemas in db")
                databases = []
                try:
                    cursor = self.run_query("show databases")
                    for row in cursor:
                        databases.append(row[1])
                except ProgrammingError as e:
                    self.logger.debug(f"Current role does not have access to run show database: {e}")
                for db in databases:
                    if db == 'SNOWFLAKE':
                        continue
                    try:
                        cursor = self.run_query(f"show schemas in {db}")
                        for row in cursor:
                            if row[1] == 'INFORMATION_SCHEMA':
                                continue
                            db_sc.append([ db, row[1] ])
                    except ProgrammingError as e:
                        self.logger.debug(f"Current role does not have access to run `show schemas in {db}`: {e}")
                        pass
                done = 1
            return db_sc

    def cursor(self, db_nm='', sc_nm=''):
        new_cursor = self.conn.cursor()
        try:
            if db_nm != '':
                new_cursor.execute(f"use database {db_nm}")
                if sc_nm != '':
                    new_cursor.execute(f"use schema {sc_nm}")
        except ProgrammingError as e:
            # Need to re-raise?
            self.logger.error(f"Fatal Error: Could not use database {db_nm} and schema {sc_nm}: {e}")
            exit(-1)
        return new_cursor

    def validate_role(self, role, db_nm, sc_nm): 
        # Validate that the role has access to the db_nm.sc_nm
        try:
            self.run_query(f"USE ROLE {role}")
            curs = self.cursor(db_nm, sc_nm)
        except: 
            self.logger.error(f"Error: role {role} does not have access to {db_nm}.{sc_nm}")
            exit(-1)
        return True
    
    def get_tables(self, clone_role, db_nm, sc_nm):
        list_of_tables = []
        try:
            self.run_query(f"USE ROLE {clone_role}")
            curs = self.cursor(db_nm, sc_nm)
            curs.execute(f"""
select tsm.id,tsm.clone_group_id,t.table_catalog, t.table_schema, t.table_name, t.table_owner,
       case
         when tsm.id = tsm.clone_group_id then FALSE
         else TRUE
       end as is_clone,
       t.is_transient, t.row_count, t.retention_time, t.bytes, tsm.active_bytes, tsm.time_travel_bytes, tsm.failsafe_bytes, tsm.retained_for_clone_bytes
  from {db_nm}.information_schema.tables t
       left outer join snowflake.account_usage.table_storage_metrics tsm on t.table_catalog = tsm.table_catalog
                                                                        and t.table_schema = tsm.table_schema
                                                                        and t.table_name = tsm.table_name
                                                                        and tsm.deleted = false
 where t.table_catalog = '{db_nm}'
   and t.table_schema = '{sc_nm}'
   and t.table_type = 'BASE TABLE'
 order by t.table_catalog asc, t.table_schema asc, t.table_name asc                         
                          """)
            for row in curs:
                list_of_tables.append(row)

        except ProgrammingError as e:
            self.logger.error(f"Fatal Error: Could not use database {db_nm} and schema {sc_nm}: {e}")
            exit(-1)
        return list_of_tables

    def get_clone_tables(self, clone_role, from_db_nm, from_sc_nm, to_db_nm, to_sc_nm):
        list_of_clones = []
        try:
            self.run_query(f"USE ROLE {clone_role}")
            curs = self.cursor(from_db_nm, from_sc_nm)
            curs.execute(f"""
with src_tables as (
  select table_catalog, table_schema, table_name, row_count, bytes, created, last_altered
    from {from_db_nm}.information_schema.tables
   where table_type    = 'BASE TABLE'
     and table_catalog = '{from_db_nm}'
     and table_schema  = '{from_sc_nm}'
), clone_tables as (
  select table_catalog, table_schema, table_name, row_count, bytes, created, last_altered
    from {to_db_nm}.information_schema.tables
   where table_type    = 'BASE TABLE'
     and table_catalog = '{to_db_nm}'
     and table_schema  = '{to_sc_nm}'
), tables as (
  select st.table_name,
         st.row_count    as st_row_count,
         ct.row_count    as ct_row_count,
         st.bytes        as st_bytes,
         ct.bytes        as ct_bytes,
         st.created      as st_created,
         ct.created      as ct_created,
         st.last_altered as st_last_altered,
         ct.last_altered as ct_last_altered,
         case
           when st_row_count != ct_row_count then TRUE
           else FALSE
         end as row_count_diff,
         case
           when st_bytes != ct_bytes then TRUE
           else FALSE
         end as bytes_diff,
         case
           when st_last_altered > ct_created then TRUE
           else FALSE
         end as dml_since_clone
    from src_tables st,
         clone_tables ct
   where st.table_name = ct.table_name
), tsm as (
  select id, clone_group_id, table_catalog, table_schema, table_name, active_bytes, retained_for_clone_bytes, deleted,
         case
           when tsm.id = tsm.clone_group_id then FALSE
           else TRUE
         end as is_clone
    from snowflake.account_usage.table_storage_metrics tsm
   where (
          (tsm.table_catalog = '{to_db_nm}' and tsm.table_schema = '{to_sc_nm}' and tsm.deleted = false)
          or
          (tsm.table_catalog = '{from_db_nm}' and tsm.table_schema = '{from_sc_nm}')
         )
)
select tsm1.table_catalog || '.' || tsm1.table_schema as clone_db_sc,
       tsm2.table_catalog || '.' || tsm2.table_schema as src_db_sc,
       tsm1.table_name,
       case
         when tsm1.active_bytes > 0 then TRUE
         else FALSE
       end as clone_active_bytes,
       case
         when tsm2.retained_for_clone_bytes > 0 then TRUE
         else FALSE
       end as src_retained_for_clone_bytes,
       tsm2.deleted as src_deleted,
       t.row_count_diff, 
       t.bytes_diff, 
       t.dml_since_clone
  from tsm tsm1, tsm tsm2, tables t
where t.table_name = tsm1.table_name
   and tsm2.id = tsm1.clone_group_id
   and tsm1.is_clone = TRUE
order by tsm1.table_name asc
-- end of query
                         """)
            for row in curs:
                list_of_clones.append(row)
        except ProgrammingError as e:
            self.logger.error(f"Fatal Error: Could not use database {from_db_nm} and schema {from_sc_nm}: {e}")
            exit(-1)
        return list_of_clones

    def get_ddl(self, db_nm, sc_nm, type, name):
        # returns non-fully qualfied name
        curs = self.cursor(db_nm, sc_nm)
        #self.logger.debug(f"select get_ddl('{type}', '{name}')")
        if type == 'DYNAMIC TABLE':
            type = 'TABLE'
        curs.execute(f"select get_ddl('{type}', '{name}')")
        obj_def = curs.fetchone()
        if obj_def is None:
            self.logger.warning("get_ddl returned no rows")
            return
        obj_src = obj_def[0]
        curs.close()
        return obj_src

    def get_objs_by_type(self, db_nm, sc_nm, type):
        # Returns an ordered list of objects for the given type in a db_nm.sc_nm
        curs = self.cursor(db_nm, sc_nm)
        objs = []
        # TODO: needs this to work with non-upper, needs to be the literal value
        # last_altered is really DML and DDL, not until Snowflake releases proper
        # last changed DDL columns will it be able to fully detect when a table
        # was altered
        try: 
            if type == 'TABLE':
                curs.execute(
                    '\n'.join(("select table_name, last_altered",
                              f"  from {db_nm}.information_schema.tables",
                               " where table_type like '%TABLE'",
                               "   and table_type not like 'EXTERNAL TABLE'",
                              f"   and table_catalog = upper('{db_nm}')",
                              f"   and table_schema  = upper('{sc_nm}')",
                               " order by table_name asc"
                    ))
                )
            elif type == 'EXTERNAL TABLE':
                type = 'TABLE'
                curs.execute(
                    '\n'.join(("select table_name, last_altered",
                              f"  from {db_nm}.information_schema.external_tables",
                              f" where table_catalog = upper('{db_nm}')",
                              f"   and table_schema  = upper('{sc_nm}')",
                               " order by table_name asc"
                    ))
                )
            elif type == 'VIEW':
                curs.execute(
                    '\n'.join(("select table_name, last_altered",
                              f"  from {db_nm}.information_schema.views",
                              f" where table_catalog = upper('{db_nm}')",
                              f"   and table_schema  = upper('{sc_nm}')",
                               " order by table_name asc"
                    ))
                )
            elif type == 'FILE_FORMAT':
                curs.execute(
                    '\n'.join(("select file_format_name, last_altered",
                              f"  from {db_nm}.information_schema.file_formats",
                              f" where file_format_catalog = upper('{db_nm}')",
                              f"   and file_format_schema  = upper('{sc_nm}')",
                               " order by file_format_name asc"
                    ))
                )
            elif type == 'SEQUENCE':
                curs.execute(
                    '\n'.join(("select sequence_name, last_altered",
                              f"  from {db_nm}.information_schema.sequences",
                              f" where sequence_catalog = upper('{db_nm}')",
                              f"   and sequence_schema  = upper('{sc_nm}')",
                               " order by sequence_name asc"
                    ))
                )                
            elif type == 'PIPE':
                curs.execute(
                    '\n'.join(("select pipe_name, last_altered",
                              f"  from {db_nm}.information_schema.pipes",
                              f" where pipe_catalog = upper('{db_nm}')",
                              f"   and pipe_schema  = upper('{sc_nm}')",
                               " order by pipe_name asc"
                    ))
                )
            # Picking the maximum last_altered date for procedures with the same name in the same schema
            # it allows us to extract all procedures and write them to the same file at once. 
            # This is a work-around for Snowflake allowing multiple stored procedures with the
            # same name, but different arguments. This causes a lot of pain for developers when
            # trying to determine what needs to be deployed in an upper environment. 
            elif type == 'PROCEDURE':
                curs.execute(
                    '\n'.join(("with last_alt as (select max(last_altered) as last_altered, procedure_name, procedure_catalog, procedure_schema ",
                              f"  from {db_nm}.information_schema.procedures",
                              f" where procedure_catalog = upper('{db_nm}') ",
                              f"   and procedure_schema = upper('{sc_nm}') ",
                               " group by procedure_name, procedure_catalog, procedure_schema)",
                               "select rs.procedure_name, rs.last_altered, p.argument_signature",
                              f"  from last_alt rs, {db_nm}.information_schema.procedures p",
                               " where p.procedure_name = rs.procedure_name",
                              f"   and p.procedure_catalog = rs.procedure_catalog and p.procedure_catalog = upper('{db_nm}')",
                              f"   and p.procedure_schema = rs.procedure_schema and p.procedure_schema = upper('{sc_nm}')",
                               " order by rs.procedure_name, p.argument_signature asc"
                    ))
                ) 
            elif type == 'FUNCTION':
                curs.execute(
                    '\n'.join(("with last_alt as (select max(last_altered) as last_altered, function_name, function_catalog, function_schema ",
                              f"  from {db_nm}.information_schema.functions",
                              f" where function_catalog = upper('{db_nm}') ",
                              f"   and function_schema = upper('{sc_nm}') ",
                               " group by function_name, function_catalog, function_schema)",
                               "select rs.function_name, rs.last_altered, f.argument_signature",
                              f"  from last_alt rs, {db_nm}.information_schema.functions f",
                               " where f.function_name = rs.function_name",
                              f"   and f.function_catalog = rs.function_catalog and f.function_catalog = upper('{db_nm}')",
                              f"   and f.function_schema = rs.function_schema and f.function_schema = upper('{sc_nm}')",
                               " order by rs.function_name, f.argument_signature asc"
                    ))
                ) 
            else:
                curs.execute(f"show {type}S in schema {db_nm}.{sc_nm}")
                curs.execute('select "name", "created_on" from table(result_scan(last_query_id()))')
            for row in curs:
                name             = row[0]
                last_modified_dt = row[1] # created_on from `show <type>S in <sc>`
                arguments        = None
                if type == 'PROCEDURE' or type == 'FUNCTION':
                    if row[2] != '()':
                        arg_array = row[2].split(' ')
                        arguments = '(' + ' '.join(arg_array[1::2])
                        # could be replaced by:
                        #arguments = '(' + ' '.join(row[2].split(' ')[1::2])
                    else:
                        arguments = '()'
                objs.append((name, last_modified_dt, arguments))
        except ProgrammingError as e:
            self.logger.error(f"Error getting objects: {e}")
            return None
        return objs
