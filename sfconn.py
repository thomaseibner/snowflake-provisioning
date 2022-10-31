import snowflake.connector as sf
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

class SfConn():

    def __init__(self, config):
        
        self.user      = config['user']
        self.account   = config['account']
        self.role      = config['role']
        self.warehouse = config['warehouse']
        self.database  = config['database']
        self.schema    = config['schema']
        self.home      = os.environ['HOME']

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
        #print("Connecting ... ")
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
            raise Exception(f"Error connecting: {e}")
        cursor = self.run_query("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")
        
    def verify_conn(self):
        curs = self.conn.cursor()
        print("Verifying connection ...")
        try:
            curs.execute("SELECT current_version(), current_role(), current_database(), current_schema(), current_warehouse()")
            one_row = curs.fetchone()
            print(f"Connected to {self.account}\nSnowflake version: {one_row[0]}\nRole: {one_row[1]}\nDB: {one_row[2]}\nSchema: {one_row[3]}\nWarehouse: {one_row[4]}")
            return True
        except Exception as e:
            print(f"Failed with: {e}")
            return False
        finally:
            curs.close()
        
    def close_conn(self):
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
            # default error message
            err_msg = f"DB Error when running query: '{query}': {e}"
            print(err_msg)
            exit(2)
            #raise sf.errors.ProgrammingError(err_msg)
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
        
