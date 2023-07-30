from sfconfig import SfConfig
import numpy as np

db_cfg_file = "db-config.json"
sc_cfg_file = "sc-config.json"
wh_cfg_file = "wh-config.json"
fr_cfg_file = "fr-config.json"

sc_map = {
    "TABLES": "TABLE",
    "EXTERNAL TABLES": "TABLE",
    "VIEWS": "VIEW",
    "MATERIALIZED VIEWS": "VIEW",
    "FILE FORMATS": "FILE FORMAT",
    "STAGES": "STAGE",
    "STREAMS": "STREAM",
    "SEQUENCES": "SEQUENCE",
    "FUNCTIONS": "FUNCTION",
    "PROCEDURES": "PROCEDURE",
    "TASKS": "TASK"
} 
            
class SfProvisionConfig():
    """SfProvisionConfig parses database/schema/warehouse configuration and 
       builds out the statements required to provision objects."""
    def __init__(self):
        try:
            self.db = SfConfig(db_cfg_file)
        except Exception as e:
            print(f"Error loading configuration from {db_cfg_file}: {str(e)}")
            exit(-1)
        try:
            self.sc = SfConfig(sc_cfg_file)
        except Exception as e:
            print(f"Error loading configuration from {sc_cfg_file}: {str(e)}")
            exit(-1)
        try:
            self.wh = SfConfig(wh_cfg_file)
        except Exception as e:
            print(f"Error loading configuration from {wh_cfg_file}: {str(e)}")
            exit(-1)
        try:
            self.fr = SfConfig(fr_cfg_file)
        except Exception as e:
            print(f"Error loading configuration from {fr_cfg_file}: {str(e)}")
            exit(-1)
        # Objects that will be populated 
        self.objects            = []
        self.drop_objects       = []
        self.obj_grants         = []
        self.revoke_obj_grants  = []
        self.owner_grants       = []
        self.role_grants        = []
        self.revoke_role_grants = []

    def db(self):
        return self.db

    def sc(self):
        return self.sc

    def wh(self):
        return self.wh

    def fr(self):
        return self.fr
    
    def validate_config(self):
        for cfg in [self.db, self.sc, self.wh]:
            if 'TYPE' not in cfg.config:
                print(f"Missing TYPE in configuration file: {cfg.filename}")
                raise ValueError
            if 'ROLE_HIERARCHY' not in cfg.config:
                print("Missing ROLE_HIERARCHY in configuration file: {cfg.filename}")
                raise ValueError
            if type(cfg.config['ROLE_HIERARCHY']) is not list:
                print("ROLE_HIERARCHY is not a list in configuration file: {cfg.filename}")
                raise ValueError
            if 'ROLE_PERMISSIONS' not in cfg.config:
                print("Missing ROLE_PERMISSIONS in configuration file: {cfg.filename}")
                raise ValueError
            if type(cfg.config['ROLE_PERMISSIONS']) is not dict:
                print("ROLE_PERMISSIONS must be a dictionary in configuration file: {cfg.filename}")
                raise ValueError
            if 'ROLE_OWNER' not in cfg.config:
                print("Missing ROLE_OWNER in configuration file: {cfg.filename}")
                raise ValueError
            if 'AR_PREFIX' not in cfg.config:
                print("Missing AR_PREFIX in configuration file: {cfg.filename}")
                raise ValueError
            # Basic checking of ROLE_HIERARCHY matching ROLE_PERMISSIONS
            for role in cfg.config['ROLE_HIERARCHY']:
                if role not in cfg.config['ROLE_PERMISSIONS']:
                    print(f"In comparing ROLE_HIERARCHY {cfg.config['ROLE_HIERARCHY']} to ROLE_PERMISSIONS: missing key {role} in {cfg.config['ROLE_PERMISSIONS']} in configuration file: {cfg.filename}")
                    raise ValueError
                if type(cfg.config['ROLE_PERMISSIONS'][role]) is not dict:
                    print(f"ROLE_PERMISSIONS for {role} {type(cfg.config['ROLE_PERMISSIONS'][role])} is not a dictionary in configuration file: {cfg.filename}")
                    raise ValueError
            # All good on basic checks
        # More basic checks but configuration specific    
        if self.db.config['TYPE'] != 'DATABASE':
            print(f"TYPE not DATABASE in configuration file: {cfg.filename}")
            raise ValueError
        if self.sc.config['TYPE'] != 'SCHEMA':
            print(f"TYPE not SCHEMA in configuration file: {cfg.filename}")
            raise ValueError
        if self.wh.config['TYPE'] != 'WAREHOUSE':
            print(f"TYPE not WAREHOUSE in configuration file: {cfg.filename}")
            raise ValueError
        # Validate that db.ROLE_HIERARCHY matches sc.ROLE_HIERARCHY
        if np.array_equal(self.db.config['ROLE_HIERARCHY'], self.sc.config['ROLE_HIERARCHY']) is False:
            print(f"ROLE_HIERARCHY between DB {self.db.config['ROLE_HIERARCHY']} and SC {self.sc.config['ROLE_HIERARCHY']} does not match in configuration files: {self.db.filename} and {self.sc.filename}")
            raise ValueError
        # Validate functional role figuration
        fr_cfg = self.fr
        for frole in fr_cfg.config:
            if 'ORDER' not in fr_cfg.config[frole]:
                print(f"Missing ORDER in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if 'INCLUDE' not in fr_cfg.config[frole]:
                print(f"Missing INCLUDE in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if 'EXCLUDE' not in fr_cfg.config[frole]:
                print(f"Missing EXCLUDE in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if 'CUSTOM_INCLUDE' not in fr_cfg.config[frole]:
                print(f"Missing CUSTOM_INCLUDE in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if 'CUSTOM_EXCLUDE' not in fr_cfg.config[frole]:
                print(f"Missing CUSTOM_EXCLUDE in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if 'SCIM_ROLES' not in fr_cfg.config[frole]:
                print(f"Missing SCIM_ROLES in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if type(fr_cfg.config[frole]['INCLUDE']) is not list:
                print(f"INCLUDE is not a list in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if type(fr_cfg.config[frole]['EXCLUDE']) is not list:
                print(f"EXCLUDE is not a list in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if type(fr_cfg.config[frole]['CUSTOM_INCLUDE']) is not list:
                print(f"CUSTOM_INCLUDE is not a list in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if type(fr_cfg.config[frole]['CUSTOM_EXCLUDE']) is not list:
                print(f"CUSTOM_EXCLUDE is not a list in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            if type(fr_cfg.config[frole]['SCIM_ROLES']) is not list:
                print(f"SCIM_ROLES is not a list in configuration for {frole} in configuration file: {fr_cfg.filename}")
                raise ValueError
            # Validate that the role types are valid for each object type based on their configuration
            for inexclude in fr_cfg.config[frole]['INCLUDE'] + fr_cfg.config[frole]['EXCLUDE']:
                if type(inexclude) is not dict:
                    print(f"{str(inexclude)} not a dict in configuration file: {fr_cfg.filename}")
                    raise ValueError
                if 'TYPE' not in inexclude:
                    print(f"Missing TYPE in configuration for {frole} in configuration file: {fr_cfg.filename}")
                    raise ValueError
                if not (inexclude['TYPE'] == 'DATABASE' or inexclude['TYPE'] == 'SCHEMA' or inexclude['TYPE'] == 'WAREHOUSE'):
                    print(f"Invalid TYPE {inexclude['TYPE']} in configuration for {frole} in configuration file: {fr_cfg.filename}")
                    raise ValueError

            for include in fr_cfg.config[frole]['INCLUDE']:
                if 'ROLE' not in include:
                    print(f"Missing ROLE in configuration for {frole} in configuration file: {fr_cfg.filename}")
                    raise ValueError
                if include['TYPE'] == 'DATABASE':
                    if include['ROLE'] not in self.db.config['ROLE_HIERARCHY']:
                        print(f"DB role {include['ROLE']} for {frole} does not exist in db ROLE_HIERARCHY {self.db.config['ROLE_HIERARCHY']} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                    if 'DATABASE' not in include:
                        print(f"Database role missing DATABASE key for {frole} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                if include['TYPE'] == 'SCHEMA':
                    if include['ROLE'] not in self.sc.config['ROLE_HIERARCHY']:
                        print(f"SC role {include['ROLE']} for {frole} does not exist in sc ROLE_HIERARCHY {self.sc.config['ROLE_HIERARCHY']} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                    if 'DATABASE' not in include:
                        print(f"Role missing DATABASE key for {frole} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                    if 'SCHEMA' not in include:
                        print(f"Role missing SCHEMA key for {frole} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                if include['TYPE'] == 'WAREHOUSE':
                    if include['ROLE'] not in self.wh.config['ROLE_HIERARCHY']:
                        print(f"WH role {include['ROLE']} for {frole} does not exist in wh ROLE_HIERARCHY {self.wh.config['ROLE_HIERARCHY']} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                    if 'WAREHOUSE' not in include:
                        print(f"Warehouse role missing WAREHOUSE key for {frole} in configuration file: {fr_cfg.filename}")
                        raise ValueError
            for exclude in fr_cfg.config[frole]['EXCLUDE']:
                if exclude['TYPE'] == 'DATABASE':
                    if 'DATABASE' not in exclude:
                        print(f"Database role missing DATABASE key for {frole} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                if exclude['TYPE'] == 'SCHEMA':
                    if 'DATABASE' not in exclude:
                        print(f"Role missing DATABASE key for {frole} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                    if 'SCHEMA' not in exclude:
                        print(f"Role missing SCHEMA key for {frole} in configuration file: {fr_cfg.filename}")
                        raise ValueError
                if exclude['TYPE'] == 'WAREHOUSE':
                    if 'WAREHOUSE' not in exclude:
                        print(f"Warehouse role missing WAREHOUSE key for {frole} in configuration file: {fr_cfg.filename}")
                        raise ValueError


                    
    def apply_cmdline_args(self, cmdline):
        config = {}
        self.cmdline = cmdline
        if cmdline.type == 'database':
            cfg = self.db
            for config_key in cfg.config['DEFAULT_DB_PARAMS']:
                config[config_key] = cfg.config['DEFAULT_DB_PARAMS'][config_key]
            for config_key in vars(cmdline.args):
                if (config_key == 'type' or config_key == 'name' or config_key == 'dryrun'):
                    continue
                if vars(cmdline.args)[config_key] is not None:
                    config[config_key.upper()] = vars(cmdline.args)[config_key]
        elif cmdline.type == 'schema':
            cfg = self.sc
            for config_key in cfg.config['DEFAULT_SC_PARAMS']:
                config[config_key] = cfg.config['DEFAULT_SC_PARAMS'][config_key]
            for config_key in vars(cmdline.args):
                if (config_key == 'type' or config_key == 'name' or config_key == 'dryrun'):
                    continue
                if vars(cmdline.args)[config_key] is not None: #and vars(cmdline.args)[config_key] is not False:
                    config[config_key.upper()] = vars(cmdline.args)[config_key]
        elif cmdline.type == 'warehouse':
            cfg = self.wh
            for config_key in cfg.config['DEFAULT_WH_PARAMS']:
                # Handle str('True')
                if type(cfg.config['DEFAULT_WH_PARAMS'][config_key]) is str:
                    if cfg.config['DEFAULT_WH_PARAMS'][config_key] == 'True':
                        config[config_key] = True
                        continue
                    elif cfg.config['DEFAULT_WH_PARAMS'][config_key] == 'False':
                        config[config_key] = False
                        continue            
                config[config_key] = cfg.config['DEFAULT_WH_PARAMS'][config_key]
            for config_key in vars(cmdline.args):
                if (config_key == 'type' or config_key == 'name' or config_key == 'dryrun'):
                    continue
                if vars(cmdline.args)[config_key] is not None: # and vars(cmdline.args)[config_key] is not False:
                    config[config_key.upper()] = vars(cmdline.args)[config_key]
            
            
        self.obj_config = config

    def create_db(self):
        cfg = self.db
        config = self.obj_config
        cmdline = self.cmdline
        create_db = []
        transient = ''
        if config['TRANSIENT'] is True:
            transient = 'TRANSIENT '
        create_db.append(f"CREATE {transient}DATABASE IF NOT EXISTS {cmdline.db_nm} ")#, end='')
        for config_key in config:
            # Properties requiring special handling
            if (config_key == 'TRANSIENT'):
                continue
            if (config_key == 'TAG'):
                create_db.append(f"  WITH TAG ( {config[config_key]} ) ")
                continue
            # Rest of the properties
            indx = 31
            if type(config[config_key]) is str:
                create_db.append(f"  {config_key:<{indx}} = '{config[config_key]}' ")#, end='')
            elif type(config[config_key]) is int:
                create_db.append(f"  {config_key:<{indx}} = {config[config_key]} ")#, end='')
            else:
                print(f"*** {config_key} unknown type: {type(config[config_key])} ***")
                raise ValueError
        create_db.append(";")
        self.objects.append("\n".join(create_db))
        self.drop_objects.append(f"DROP DATABASE IF EXISTS {cmdline.db_nm};")
        self.owner_grants.append(f"GRANT OWNERSHIP ON DATABASE {cmdline.db_nm} TO ROLE {cfg.config['ROLE_OWNER']} REVOKE CURRENT GRANTS;")
        self.owner_grants.append(f"GRANT ALL PRIVILEGES ON DATABASE {cmdline.db_nm} TO ROLE {cfg.config['ROLE_OWNER']};")
        
    def create_db_roles(self):
        cfg = self.db
        cmdline = self.cmdline
        ar_db_prefix = cfg.config['AR_PREFIX']
        for role_type in cfg.config['ROLE_HIERARCHY'][::-1]:
            ar_role = f"{ar_db_prefix}{cmdline.db_nm}_{role_type}_AR"
            self.create_role(ar_role)

    def create_db_grants(self):
        cfg = self.db
        cmdline = self.cmdline
        ar_db_prefix = cfg.config['AR_PREFIX']
        for role_type in cfg.config['ROLE_HIERARCHY'][::-1]:
            ar_role = f"{ar_db_prefix}{cmdline.db_nm}_{role_type}_AR"
            type_grants = cfg.config['ROLE_PERMISSIONS'][role_type]
            for privilege in type_grants.keys():
                for object in type_grants[privilege]:
                    if (object == 'DATABASE'):
                        self.grant_privilege(privilege, object, cmdline.db_nm, ar_role)
                    elif (object == 'SCHEMA'):
                        print(f"Unsupported grants for {privilege} on {object}")
                    else:
                        self.grant_privilege_in(privilege, object, 'ON DATABASE', cmdline.db_nm, ar_role)
                        self.grant_future_privilege_in(privilege, object, 'ON DATABASE', cmdline.db_nm, ar_role)
        
    def create_db_r2r_grants(self):
        cfg = self.db
        cmdline = self.cmdline
        role_hierarchy = cfg.config['ROLE_HIERARCHY']
        ar_db_prefix = cfg.config['AR_PREFIX']
        hierarchy_length = len(role_hierarchy)
        hierarchy_length = hierarchy_length - 1;
        for role_num in range(hierarchy_length)[::-1]:
            lower_ar_role = f"{ar_db_prefix}{cmdline.db_nm}_{role_hierarchy[role_num+1]}_AR"
            higher_ar_role = f"{ar_db_prefix}{cmdline.db_nm}_{role_hierarchy[role_num]}_AR"
            self.grant_r2r(lower_ar_role, higher_ar_role)
            # Grant the highest level role to 'ROLE_OWNER'
        self.grant_r2r(f"{ar_db_prefix}{cmdline.db_nm}_{role_hierarchy[0]}_AR", cfg.config['ROLE_OWNER'])
            
    def create_sc(self):
        cfg = self.sc
        config = self.obj_config
        cmdline = self.cmdline
        create_sc = []
        transient = ''
        if config['TRANSIENT'] is True:
            transient = 'TRANSIENT '
        create_sc.append(f"CREATE {transient}SCHEMA IF NOT EXISTS {cmdline.db_nm}.{cmdline.sc_nm} ")#, end='')
        for config_key in config:
            # Properties requiring special handling
            if (config_key == 'TRANSIENT'):
                continue
            if (config_key == 'TAG'):
                create_sc.append(f"  WITH TAG ( {config[config_key]} ) ")
                continue
            if (config_key == 'MANAGED_ACCESS'):
                if (config[config_key] is True):
                    create_sc.append(f"  WITH MANAGED ACCESS ")
                continue
            # Rest of the properties
            indx = 31
            if type(config[config_key]) is str:
                create_sc.append(f"  {config_key:<{indx}} = '{config[config_key]}' ")#, end='')
            elif type(config[config_key]) is int:
                create_sc.append(f"  {config_key:<{indx}} = {config[config_key]} ")#, end='')
            else:
                print(f"*** {config_key} unknown type: {type(config[config_key])} ***")
        create_sc.append(";")
        self.objects.append("\n".join(create_sc))
        self.drop_objects.append(f"DROP SCHEMA IF EXISTS {cmdline.db_nm}.{cmdline.sc_nm};")
        self.owner_grants.append(f"GRANT OWNERSHIP ON SCHEMA {cmdline.db_nm}.{cmdline.sc_nm} TO ROLE {cfg.config['ROLE_OWNER']} REVOKE CURRENT GRANTS;")
        self.owner_grants.append(f"GRANT ALL PRIVILEGES ON SCHEMA {cmdline.db_nm}.{cmdline.sc_nm} TO ROLE {cfg.config['ROLE_OWNER']};")

    def create_sc_roles(self):
        cfg = self.sc
        cmdline = self.cmdline
        ar_sc_prefix = cfg.config['AR_PREFIX']
        for role_type in cfg.config['ROLE_HIERARCHY'][::-1]:
            ar_role = f"{ar_sc_prefix}{cmdline.db_nm}_{cmdline.sc_nm}_{role_type}_AR"
            self.create_role(ar_role)

    def create_sc_grants(self):
        seen = {}
        cfg = self.sc
        cmdline = self.cmdline
        ar_sc_prefix = cfg.config['AR_PREFIX']
        ar_db_prefix = self.db.config['AR_PREFIX']
        once = 1
        for role_type in cfg.config['ROLE_HIERARCHY'][::-1]:
            ar_role = f"{ar_sc_prefix}{cmdline.db_nm}_{cmdline.sc_nm}_{role_type}_AR"
            db_ar_role = f"{ar_db_prefix}{cmdline.db_nm}_{role_type}_AR"
            if (once == 1):
                # One grant at the parent database level is enough for
                # all the roles inheriting from this role to see the db
                self.grant_privilege('USAGE', 'DATABASE', cmdline.db_nm, ar_role)
                self.grant_privilege('USAGE', 'SCHEMA', f"{cmdline.db_nm}.{cmdline.sc_nm}", ar_role)
                once = 0
            self.grant_r2r(ar_role, db_ar_role)
            type_grants = cfg.config['ROLE_PERMISSIONS'][role_type]
            for privilege in type_grants.keys():
                for object in type_grants[privilege]:
                    if privilege == 'ALL':
                        # remove S; materialized/external and skip multiples
                        sing_obj = sc_map[object]
                        if sing_obj not in seen.keys():
                            self.grant_create_privilege(sing_obj, f"ON SCHEMA {cmdline.db_nm}.{cmdline.sc_nm}", ar_role)
                            seen[sing_obj] = 1
                    self.grant_privilege_in(privilege, f"ALL {object}", f"{cmdline.db_nm}.{cmdline.sc_nm}", 'ON SCHEMA', ar_role)
                    self.grant_future_privilege_in(privilege, object, f"{cmdline.db_nm}.{cmdline.sc_nm}", 'ON SCHEMA', ar_role)
        
    def create_sc_r2r_grants(self):
        cfg = self.sc
        cmdline = self.cmdline
        ar_sc_prefix = cfg.config['AR_PREFIX']
        role_hierarchy = cfg.config['ROLE_HIERARCHY']
        hierarchy_length = len(role_hierarchy)
        hierarchy_length = hierarchy_length - 1;
        for role_num in range(hierarchy_length)[::-1]:
            lower_ar_role = f"{ar_sc_prefix}{cmdline.db_nm}_{cmdline.sc_nm}_{role_hierarchy[role_num+1]}_AR"
            higher_ar_role = f"{ar_sc_prefix}{cmdline.db_nm}_{cmdline.sc_nm}_{role_hierarchy[role_num]}_AR"
            self.grant_r2r(lower_ar_role, higher_ar_role)
            # Grant the highest level role to 'ROLE_OWNER'
        self.grant_r2r(f"{ar_sc_prefix}{cmdline.db_nm}_{cmdline.sc_nm}_{role_hierarchy[0]}_AR", cfg.config['ROLE_OWNER']) 
            
    def create_wh(self):
        cfg = self.wh
        config = self.obj_config
        cmdline = self.cmdline
        create_wh = []
        create_wh.append(f"CREATE WAREHOUSE IF NOT EXISTS {cmdline.wh_nm} ")
        for config_key in sorted(config):
            # Properties requiring special handling
            if (config_key == 'RESOURCE_MONITOR' or config_key == 'SCALING_POLICY'):
                create_wh.append(f"  {config_key:<35} = {config[config_key]} ")
                continue
            if (config_key == 'TAG'): 
                create_wh.append(f"  WITH TAG ( {config[config_key]} ) ")
                continue
            # Rest of the properties
            indx = 35
            if type(config[config_key]) is str:
                create_wh.append(f"  {config_key:<{indx}} = '{config[config_key]}' ")
            elif type(config[config_key]) is int:
                create_wh.append(f"  {config_key:<{indx}} = {config[config_key]} ")
            elif type(config[config_key]) is bool:
                create_wh.append(f"  {config_key:<{indx}} = {str(config[config_key]).upper()} ")
            else:
                print(f"*** {config_key} unknown type: {type(config[config_key])} ***")
        create_wh.append(";")
        self.objects.append("\n".join(create_wh))
        self.drop_objects.append(f"DROP WAREHOUSE IF EXISTS {cmdline.wh_nm};")
        self.owner_grants.append(f"GRANT OWNERSHIP ON WAREHOUSE {cmdline.wh_nm} TO ROLE {cfg.config['ROLE_OWNER']} REVOKE CURRENT GRANTS;")
        self.owner_grants.append(f"GRANT ALL PRIVILEGES ON WAREHOUSE {cmdline.wh_nm} TO ROLE {cfg.config['ROLE_OWNER']};")

    def create_wh_roles(self):
        cfg = self.wh
        cmdline = self.cmdline
        ar_wh_prefix = cfg.config['AR_PREFIX']
        for role_type in cfg.config['ROLE_HIERARCHY'][::-1]:
            ar_role = f"{ar_wh_prefix}{cmdline.wh_nm}_{role_type}_AR"
            self.create_role(ar_role)

    def create_wh_grants(self):
        cfg = self.wh
        cmdline = self.cmdline
        ar_wh_prefix = cfg.config['AR_PREFIX']
        for role_type in cfg.config['ROLE_HIERARCHY'][::-1]:
            ar_role = f"{ar_wh_prefix}{cmdline.wh_nm}_{role_type}_AR"
            type_grants = cfg.config['ROLE_PERMISSIONS'][role_type]
            for privilege in type_grants.keys():
                for object in type_grants[privilege]:
                    self.grant_privilege(privilege, object, cmdline.wh_nm, ar_role)
        
    def create_wh_r2r_grants(self):
        cfg = self.wh
        cmdline = self.cmdline
        ar_wh_prefix = cfg.config['AR_PREFIX']
        role_hierarchy = cfg.config['ROLE_HIERARCHY']
        hierarchy_length = len(role_hierarchy)
        hierarchy_length = hierarchy_length - 1;
        for role_num in range(hierarchy_length)[::-1]:
            lower_ar_role = f"{ar_wh_prefix}{cmdline.wh_nm}_{role_hierarchy[role_num+1]}_AR"
            higher_ar_role = f"{ar_wh_prefix}{cmdline.wh_nm}_{role_hierarchy[role_num]}_AR"
            self.grant_r2r(lower_ar_role, higher_ar_role)
        # Grant the highest level role to 'ROLE_OWNER'
        self.grant_r2r(f"{ar_wh_prefix}{cmdline.wh_nm}_{role_hierarchy[0]}_AR", cfg.config['ROLE_OWNER'])
        
    def create_role(self, role):
        self.objects.append(f"CREATE ROLE IF NOT EXISTS {role};")
        if self.cmdline.type == 'database':
            self.owner_grants.append(f"GRANT OWNERSHIP ON ROLE {role} TO ROLE {self.db.config['ROLE_OWNER']} REVOKE CURRENT GRANTS;")
        elif self.cmdline.type == 'schema':
            self.owner_grants.append(f"GRANT OWNERSHIP ON ROLE {role} TO ROLE {self.sc.config['ROLE_OWNER']} REVOKE CURRENT GRANTS;")
        elif self.cmdline.type == 'warehouse':
            self.owner_grants.append(f"GRANT OWNERSHIP ON ROLE {role} TO ROLE {self.wh.config['ROLE_OWNER']} REVOKE CURRENT GRANTS;")
        self.drop_objects.append(f"DROP ROLE IF EXISTS {role};")

    def grant_r2r(self, l_ar, h_ar):
        self.role_grants.append(f"GRANT ROLE {l_ar} TO ROLE {h_ar};")
        self.revoke_role_grants.append(f"REVOKE ROLE {l_ar} FROM ROLE {h_ar};")
        
    def grant_privilege(self, priv, on, name, role):
        self.obj_grants.append(f"GRANT {priv} ON {on} {name} TO ROLE {role};")
        self.revoke_obj_grants.append(f"REVOKE {priv} ON {on} {name} FROM ROLE {role};")

    def grant_privilege_in(self, priv, on, in_obj, name, role):
        self.obj_grants.append(f"GRANT {priv} ON {on} {name} {in_obj} TO ROLE {role};")
        self.revoke_obj_grants.append(f"REVOKE {priv} ON {on} {name} {in_obj} FROM ROLE {role};")

    def grant_future_privilege(self, priv, on, name, role):
        self.obj_grants.append(f"GRANT {priv} ON FUTURE {on} {name} TO ROLE {role};")
        self.revoke_obj_grants.append(f"REVOKE {priv} ON FUTURE {on} {name} FROM ROLE {role};")

    def grant_future_privilege_in(self, priv, on, in_obj, name, role):
        self.obj_grants.append(f"GRANT {priv} ON FUTURE {on} {name} {in_obj} TO ROLE {role};")
        self.revoke_obj_grants.append(f"REVOKE {priv} ON FUTURE {on} {name} {in_obj} FROM ROLE {role};")

    def grant_create_privilege(self, obj, in_obj, role):
        self.obj_grants.append(f"GRANT CREATE {obj} {in_obj} TO ROLE {role};")
        self.revoke_obj_grants.append(f"REVOKE CREATE {obj} {in_obj} FROM ROLE {role};")

    def print_create(self):
        for obj in self.objects:
            print(obj)
        print()
        for owner in self.owner_grants:
            print(owner)
        print()
        for role in self.role_grants:
            print(role)
        print()
        for grants in self.obj_grants:
            print(grants)

    def print_drop(self):
        for grant_revokes in self.revoke_obj_grants[::-1]:
            print(grant_revokes)
        print()
        for role_revoke in self.revoke_role_grants[::-1]:
            print(role_revoke)
        print()
        for drops in self.drop_objects[::-1]:
            print(drops)
        
    def print_config(self):
        for obj in self.objects:
            print(obj)
        print()
        for owner in self.owner_grants:
            print(owner)
        print()
        for role in self.role_grants:
            print(role)
        print()
        for grants in self.obj_grants:
            print(grants)
        print()
        for grant_revokes in self.revoke_obj_grants[::-1]:
            print(grant_revokes)
        print()
        for role_revoke in self.revoke_role_grants[::-1]:
            print(role_revoke)
        print()
        for drops in self.drop_objects[::-1]:
            print(drops)
        


            
if __name__ == "__main__":
    prov_cfg = SfProvisionConfig()
    # Validates the configuration
    try:
        prov_cfg.validate_config()
    except ValueError:
        print("Configuration problem exiting")
        exit(-1)

