import re
from snowflake.connector.errors import ProgrammingError

class SfFuncRole():
    def __init__(self, cfg, sf_conn):
        self.cfg = cfg
        self.sf_conn = sf_conn
        prov_cfg = cfg
        db_cfg = prov_cfg.db
        sc_cfg = prov_cfg.sc
        wh_cfg = prov_cfg.wh
        fr_cfg = prov_cfg.fr

        
    def list_roles(self):
        return self.cfg.fr.config

    def gen_role_delta(self, frole):
        include_grants = []
        exclude_grants = []
        include_frs = []
        exclude_frs = []

        db_prefix = db_cfg.config['AR_PREFIX']
        sc_prefix = sc_cfg.config['AR_PREFIX']
        wh_prefix = wh_cfg.config['AR_PREFIX']

        # Includes/excludes are really hard to match so the scripts needs to compare the output of the DB to
        # what is generated with the configuration. It will not be a simple 1:1 for between config and db

        # For DB/SC/WH
        existing_ar = []
        # For roles
        existing_fr = []    
        existing_role = 0

        try:
            cursor = self.sf_conn.run_query(f"SHOW GRANTS TO ROLE {frole}")
            for row in cursor:
                existing_role = 1
                if row[1] != 'OWNERSHIP':
                    if row[1] == 'USAGE':
                        match_type = re.search(r'_(\w{2,3})_AR$', row[3])
                        if match_type:
                            existing_ar.append(row[3])
                        match_type = re.search(r'_FR$', row[3])
                        if match_type:
                            existing_fr.append(row[3])
                    else:
                        # Not _ARs or _FRs 
                        # How can we fix this?
                        print("Error cannot generate configuration for row: ", row)
        except ProgrammingError as e:
            print(f"-- error when fetching grants for {frole} -- role does not exist: {e.errno}")
        except:
            print(f"-- error when fetching grants for {frole} -- role does not exist")
        # if existing_role = 1
        if existing_role == 0:
            print(f"CREATE ROLE {frole} IF NOT EXISTS;")

        # What does the configuration say should be granted?
        # FIX: db *
        # Build up include statements
        for include in self.cfg.fr.config[frole]['INCLUDE']:
            if include['TYPE'] == 'DATABASE':
                include_grants.append(f"select distinct 'DATABASE' as type, grantee_name, db_name from allfuturegrantsdb where db_name like '{include['DATABASE']}' and grantee_name like '{db_prefix}%_{include['ROLE']}_AR' order by grantee_name asc")
            elif include['TYPE'] == 'SCHEMA':
                include_grants.append(f"select distinct 'SCHEMA' as type, grantee_name, db_name, sc_name, db_sc\n  from allfuturegrantssc where db_name like '{include['DATABASE']}' and sc_name like '{include['SCHEMA']}' and grantee_name like '{sc_prefix}%_{include['ROLE']}_AR' order by grantee_name asc")
            elif include['TYPE'] == 'WAREHOUSE':
                # We need something that gives us all role grants for warehouses
                include_grants.append(f"select distinct 'WAREHOUSE' as type, grantee_name, wh_name\n  from allgrantswh where wh_name like '{include['WAREHOUSE']}' and grantee_name like '{wh_prefix}%_{include['ROLE']}_AR' order by grantee_name asc")
            elif include['TYPE'] == 'ROLE':
                include_frs.append(f"select distinct 'ROLE' as type, name from snowflake.account_usage.roles where name like '{include['ROLE']}' and deleted_on is null order by name asc")
            else:
                print(f"Type incorrect: {include['TYPE']}")
                exit(-1)
        # Build up exclude statements
        for exclude in self.cfg.fr.config[frole]['EXCLUDE']:
            if exclude['TYPE'] == 'DATABASE':
                exclude_grants.append(f"select distinct 'DATABASE' as type, grantee_name, db_name from allfuturegrantsdb where db_name like '{exclude['DATABASE']}' order by grantee_name asc")
            elif exclude['TYPE'] == 'SCHEMA':
                exclude_grants.append(f"select distinct 'SCHEMA' as type, grantee_name, db_name, sc_name, db_sc\n  from allfuturegrantssc where db_name like '{exclude['DATABASE']}' and sc_name like '{exclude['SCHEMA']}' order by grantee_name asc")
            elif exclude['TYPE'] == 'WAREHOUSE':
                # We need something that gives us all role grants for warehouses
                excludes_grants.append(f"select distinct 'WAREHOUSE' as type, grantee_name, wh_name\n  from allgrantswh where wh_name like '{exclude['WAREHOUSE']}' order by grantee_name asc")
            elif include['TYPE'] == 'ROLE':
                excludes_frs.append(f"select distinct 'ROLE' as type, name from snowflake.account_usage.roles where name like '{exclude['ROLE']}' and deleted_on is null order by name asc")            
            else:
                print(f"Type incorrect: {include['TYPE']}")
                exit(-1)
    
        # type = 'ROLE' should only be _FR?

        # [x] we need to split out grants vs roles
        # includes_grants vs includes_fr
        # excludes_grants vs excludes_fr

        map_roles = {}
        inc_grants = []
        inc_frs = []
        exc_grants = []
        exc_frs = []

        for sql in include_grants:
            cursor = self.sf_conn.run_query(sql)
            for row in cursor:
                inc_grants.append(row[1])
                map_roles[row[0]] = row
        for sql in exclude_grants:
            cursor = self.sf_conn.run_query(sql)
            for row in cursor:
                exc_grants.append(row[1])
                map_roles[row[0]] = row
        for sql in include_frs:
            cursor = self.sf_conn.run_query(sql)
            for row in cursor:
                inc_frs.append(row[1])
                map_roles[row[0]] = row    
        for sql in exclude_frs:
            cursor = self.sf_conn.run_query(sql)
            for row in cursor:
                exc_frs.append(row[1])
                map_roles[row[0]] = row    
            
        # CUSTOM_INCLUDE/CUSTOM_EXCLUDE
        # what if they aren't valid roles?
        for role in self.cfg.fr.config[frole]['CUSTOM_INCLUDE']:
            match_type = re.search(r'_AR$', role)
            if match_type:
                inc_grants.append(role)
            match_type = re.search(r'_FR$', role)
            if match_type:
                inc_frs.append(role)
        for role in self.cfg.fr.config[frole]['CUSTOM_EXCLUDE']:
            match_type = re.search(r'_AR$', role)
            if match_type:
                exc_grants.append(role)
            match_type = re.search(r'_FR$', role)
            if match_type:
                exc_frs.append(role)

        # ORDER: INCEXC vs EXCINC
        # depending on order include/exclude first
        new_grants = []
        new_frs = []
        for inc in inc_grants:
            if inc not in exc_grants:
                new_grants.append(inc)
        for inc in inc_frs:
            if inc not in exc_frs:
                new_frs.append(inc)

        # now validate between new_grants and existing_ar
        # -"-                  new_frs    and existing_fr
        delta_add_grants = []
        delta_add_frs = []
        for new in new_grants:
            if new not in existing_ar:
                delta_add_grants.append(new)
        for new in new_frs:
            if new not in existing_fr:
                delta_add_frs.append(new)
    
        delta_remove_grants = []
        delta_remove_frs = []
        for old in existing_ar:
            if old not in new_grants:
                delta_remove_grants.append(old)
        for old in existing_fr:
            if old not in new_frs:
                delta_remove_frs.append(old)

        for addgrant in delta_add_grants:
            print(f"GRANT ROLE {addgrant} TO ROLE {frole};")

        for addfr in delta_add_frs:
            print(f"GRANT ROLE {addfr} TO ROLE {frole};")

        for remgrant in delta_remove_grants:
            print(f"REVOKE ROLE {remgrant} FROM ROLE {frole};")

        for remfr in delta_remove_frs:
            print(f"REVOKE ROLE {remfr} FROM ROLE {frole};")

        print()

        
        

