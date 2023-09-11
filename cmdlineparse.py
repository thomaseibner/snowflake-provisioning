import argparse
from sfvalidator import SfValidator

def time_travel_validate(string):
    num = int(string)
    if (num > 0 and num < 91):
        return num
    raise ValueError

def warehouse_size_validate(string):
    size = string.upper()
    if size in ['XSMALL','X-SMALL','SMALL','MEDIUM','LARGE','XLARGE','X-LARGE','XXLARGE','X2LARGE','2X-LARGE','XXXLARGE','X3LARGE','3X-LARGE','4XLARGE','4X-LARGE','5XLARGE','5X-LARGE','6XLARGE','6X-LARGE']:
        return size
    raise ValueError

def cluster_size_validate(string):
    size = int(string)
    if (size > 0 and size < 11):
        return size
    raise ValueError

def auto_suspend_validate(string):
    num = int(string)
    if (num == 0 or num >= 60):
        return num
    raise ValueError

def db_sc_validate(string):
    sf_val = SfValidator()
    db_nm,sc_nm = sf_val.split_db_sc(string)
    return [ db_nm, sc_nm ]

class CmdlineParseExport():
    def __init__(self):
        parser = argparse.ArgumentParser(description='Snowflake Object Export Utility')
        parser.add_argument('--list', action='store_true', help='List all Database.Schema in account')
        parser.add_argument('--all', action='store_true', help='Export all Database.Schema')
        parser.add_argument('--export_dir', type=str, default='./export', help='Name of base directory to export to')
        parser.add_argument('--log_level', type=str, choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'], default='INFO', help='Log Level to output')
        parser.add_argument('--delete', action='store_true', help='Delete files no longer present in schema')
        parser.add_argument('--database_schema', '--db_sc', type=db_sc_validate, help='Name(s) of Database.Schema to export', nargs='+')

        self.parser = parser
        self.args = parser.parse_args()
        self.sf_val = SfValidator()
        self.checkinput()
        # decide later on below:
        #if self.args.database_schema:
        #    self.db_sc = self.args.database_schema
        #else:
        #    self.db_sc = []

    def checkinput(self):
        # db_sc needs to be checked, even if we use a different function for it
        # database_schema should actually be populated and not just the global variable from main script
        if self.args.list is not True and self.args.all is not True and self.args.database_schema is None:
            self.parser.print_help()
            exit(0)
        if self.args.list is True and self.args.all is True:
            print("Cannot use both --all and --list argument at the same time")
            self.parser.print_help()
            exit(0)
        if self.args.database_schema is not None and self.args.list is True:
            print("Cannot specify --database_schema and --list at the same time")
            self.parser.print_help()
            exit(0)
        if self.args.database_schema is not None and self.args.all is True:
            print("Cannot specify --database_schema and --all at the same time")
            self.parser.print_help()
            exit(0)
        # transfer how-ever database_schema looks like into the array db_sc from main program, but stupid to do this twice
        # can we convert to same datamodel?

class CmdlineParseClone():
    def __init__(self):
        parser = argparse.ArgumentParser(description='Snowflake Clone Utility')
        # the clone_role has to have access to both clone the original tables and to write to the to_db_sc
        parser.add_argument('--log_level', type=str, choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'], default='INFO', help='Log Level to output')
        sub_parsers = parser.add_subparsers(help='sub-command help', dest='type')   
        init_parser    = sub_parsers.add_parser('init', help='Initialize clones of target schema tables in source schema')
        refresh_parser = sub_parsers.add_parser('refresh', help='Refresh clones of tables in target schema from source schema')
        remove_parser  = sub_parsers.add_parser('remove', help='Remove clones in target schema derived from source schema')
        for tmp_parser in [init_parser, refresh_parser, remove_parser]:
            tmp_parser.add_argument('--owner_role', type=str, help='Name of role to grant ownership of objects to in target schema')
            tmp_parser.add_argument('--from_db_sc', type=db_sc_validate, help='Name of source Database.Schema')
            tmp_parser.add_argument('--to_db_sc', type=db_sc_validate, help='Name of target Database.Schema')
            tmp_parser.add_argument('--dryrun', '--noapply', action='store_true', help='Do not apply any changes - print on stdout')
        for tmp_parser in [init_parser, refresh_parser]:
            tmp_parser.add_argument('--clone_role', type=str, help='Name of role to grant to perform clone with') 
            # in case owner is not the same as role (example: clone role can read prod and write dev, owner role can only write dev)
        init_parser.add_argument('--delete_existing', action='store_true', help='Delete existing tables in target schema - if not fails if table exists already')
        self.parser = parser
        self.args = parser.parse_args()
        self.sf_val = SfValidator()
        self.checkinput()

    def checkinput(self):
        type = self.args.type
        if (type is None):
            self.parser.print_help()
            print("Please specify type: init/refresh/remove")
            exit(0)
        self.type = type
        # validate to_db_sc and from_db_sc
        if self.args.to_db_sc is None or self.args.from_db_sc is None:
            print("To and From Database.Schema must be specified")
            self.parser.print_help()
            exit(0)

        for quoted_identifier in ['.'.join(self.args.to_db_sc), '.'.join(self.args.from_db_sc)]:
            schema_match = self.sf_val.schema_parse(quoted_identifier)
            if (schema_match['error'] == 1):
                print(f"Error: {schema_match['error_text']}")
                exit(-1)
            if (schema_match['quoted_database'] is True): 
                print(f"Database {schema_match['database']} has quotes, unsupported for now")
                exit(-1)
            if (schema_match['quoted_schema'] is True):
                print(f"Schema {schema_match['schema']} has quotes, unsupported for now")
                exit(-1)
        # now we can use the unquoted names
        db_nm,sc_nm = self.args.to_db_sc[0].upper(), self.args.to_db_sc[1].upper()
        self.to_db_nm = db_nm
        self.to_sc_nm = sc_nm
        db_nm,sc_nm = self.args.from_db_sc[0].upper(), self.args.from_db_sc[1].upper()
        self.from_db_nm = db_nm
        self.from_sc_nm = sc_nm

        if (type == 'init'):
            # clone_role, delete_existing - don't need to check delete_existing
            if self.args.clone_role is None:
                print("Clone role must be specified")
                self.parser.print_help()
                exit(0)
            self.clone_role = self.args.clone_role
        elif (type == 'refresh'):
            # clone_role
            if self.args.clone_role is None:
                print("Clone role must be specified")
                self.parser.print_help()
                exit(0)
            self.clone_role = self.args.clone_role
        #elif (type == 'remove '):
        #    # nothing special here
        #    print()
        else:
            # This should never happen with the choices provided in the argparse init
            print(f"Unknown type: {type}")
        # validate rest of arguments that are similar across all types 
        if self.args.owner_role is None:
            print("Owner role must be specified")
            self.parser.print_help()
            exit(0)
        self.owner_role = self.args.owner_role
        # Good to go now

class CmdlineParseCreateDrop():
    """CmdlineParse parses the command line options required to provision
       a database/schema/warehouse and validates the parameters are valid."""
    def __init__(self):
        parser = argparse.ArgumentParser(description='Snowflake database, schema, and warehouse provisioning')
        sub_parsers = parser.add_subparsers(help='sub-command help', dest='type')

        db_parser = sub_parsers.add_parser('database', help='Provision database in Snowflake')
        sc_parser = sub_parsers.add_parser('schema', help='Provision schema in Snowflake')
        wh_parser = sub_parsers.add_parser('warehouse', help='Provision warehouse in Snowflake')

        for tmp_parser in [db_parser, sc_parser, wh_parser]:
            tmp_parser.add_argument('name', type=str, help='Name of Snowflake object to provision')
#            tmp_parser.add_argument('--dryrun', '--noapply', action='store_true', help='Do not apply any changes - print on stdout')

        for tmp_parser in [db_parser, sc_parser]:
            tmp_parser.add_argument('--data_retention_time_in_days', type=time_travel_validate, default=1, help='Time Travel in days 1 for standard edition 1-90 for higher editions')
            tmp_parser.add_argument('--max_data_extension_time_in_days', type=time_travel_validate, help='Maximum number of days Snowflake can extend data retention period 1-90')
            tmp_parser.add_argument('--transient', action='store_true',help='Applies to database and schema, creates transient object')
            tmp_parser.add_argument('--default_ddl_collation', type=str, help='Default DDL Collation')

        wh_parser.add_argument('--warehouse_size', type=warehouse_size_validate, help='Warehouse Size: XSmall - X6Large')
        wh_parser.add_argument('--max_cluster_count', type=cluster_size_validate, default=1, help='Warehouse Max Cluster Count 1 for standard edition, 1-10 for higher editions')
        wh_parser.add_argument('--min_cluster_count', type=cluster_size_validate, default=1, help='Warehouse Min Cluster Count 1 for standard edition, 1-10 for higher editions')
        wh_parser.add_argument('--scaling_policy', type=str, default='STANDARD', choices=['STANDARD','ECONOMY'], help='Warehouse Scaling Policy: STANDARD or ECONOMY')
        wh_parser.add_argument('--auto_suspend', type=auto_suspend_validate, default=60, help='Warehouse Auto-Suspend: Minimum 60 seconds, 0 to never suspend')
        wh_parser.add_argument('--auto_resume', action='store_false', help='Warehouse Auto-Resume: FALSE by including this parameter TRUE by not including it')
        wh_parser.add_argument('--initially_suspended', action='store_false', help='Warehouse Initially Suspended: FALSE by including this parameter TRUE by not including it')
        wh_parser.add_argument('--resource_monitor', type=str, help='Name of resource monitor to add warehouse to')
        wh_parser.add_argument('--enable_query_acceleration', action='store_false', help='Enable query acceleration: FALSE by including this parameter TRUE by not including it')
        wh_parser.add_argument('--query_acceleration_max_scale_factor', type=int, help='Query acceleration max scale factorn: 0-100, default: 8')
        wh_parser.add_argument('--max_concurrency_level', type=int, help='Max concurrency level')
        wh_parser.add_argument('--statement_queued_timeout_in_seconds', type=int, help='Statement Queued Timeout in Seconds')
        wh_parser.add_argument('--statement_timeout_in_seconds', type=int, help='Statement Timeout in Seconds')

        sc_parser.add_argument('--managed_access', action='store_true', help='With managed access enabled')
        
        for tmp_parser in [db_parser, sc_parser, wh_parser]:
            tmp_parser.add_argument('--comment', type=str, help='Comment to add to object')
            tmp_parser.add_argument('--tag', type=str, help='Add a single tag_name=value to object')

        self.parser = parser
        self.args = parser.parse_args()
        self.sf_val = SfValidator()
        self.checkinput()

    def checkinput(self):
        type = self.args.type
        if (type is None):
            self.parser.print_help()
            print("Please specify type: database/schema/warehouse")
            exit(0)
        self.type = type
        # name is a required argument so parser figures this out
        name = self.args.name
        
        if (type == 'database'):
            db_match = self.sf_val.db_parse(name)
            if (db_match['error'] == 1):
                print(f"Error: {db_match['error_text']}")
                exit(-1)
            if (db_match['quoted_database'] is True): 
                print(f"Database {db_match['database']} has quotes, unsupported for now")
                exit(-1)

            # Need to uppercase this
            db_nm = db_match['database'].upper()
            self.db_nm = db_nm

        elif (type == 'schema'):
            # all this needs to be done in the validator 
            schema_match = self.sf_val.schema_parse(name)
            if (schema_match['error'] == 1):
                print(f"Error: {schema_match['error_text']}")
                exit(-1)
            if (schema_match['quoted_database'] is True): 
                print(f"Database {schema_match['database']} has quotes, unsupported for now")
                exit(-1)
            if (schema_match['quoted_schema'] is True):
                print(f"Schema {schema_match['schema']} has quotes, unsupported for now")
                exit(-1)

            db_nm,sc_nm = schema_match['database'].upper(), schema_match['schema'].upper()
            self.db_nm = db_nm
            self.sc_nm = sc_nm

        elif (type == 'warehouse'):
            wh_match = self.sf_val.wh_parse(name)
            if (wh_match['error'] == 1):
                print(f"Error: {wh_match['error_text']}")
                exit(-1)
            if (wh_match['quoted_warehouse'] is True): 
                print(f"Warehouse {wh_match['warehouse']} has quotes, unsupported for now")
                exit(-1)

            wh_nm = wh_match['warehouse'].upper()
            self.wh_nm = wh_nm
            # Need to validate resource_monitor is a valid name
            resource_monitor = self.args.resource_monitor
            if (resource_monitor is not None):
                name_match = self.sf_val.name_parse(resource_monitor)
                if name_match['error'] == 1:
                    print(f"resource_monitor error: {name_match['error_text']}")
                    exit(-1)
                if name_match['name'] != resource_monitor:
                    print(f"resource monitor name not a valid Snowflake object name: {resource_monitor} != {name_match['name']}")
                    exit(-1)
                    
                # Resource monitor name is fine, it can be quoted too.
        else:
            # This should never happen with the choices provided in the argparse init
            print(f"Unknown type: {type}")


if __name__ == "__main__":
    cmdline = CmdlineParse()
    print(cmdline.args)
    for opt in vars(cmdline.args):
        print(f"{opt}:{vars(cmdline.args)[opt]}")



