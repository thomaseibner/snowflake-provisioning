import re

class SfGenConfig():
    def __init__(self, cfg, sf_conn, roles):
        self.cfg = cfg
        self.sf_conn = sf_conn
        self.generated_config = []
        self.roles = []

        if len(roles) == 1 and roles[0] == 'ALL':
            # load up all FRs
            cursor = sf_conn.run_query("show roles like '%_FR'")
            for row in cursor:
                roles.append(row[1])
            self.roles = roles
        else:
            self.roles = roles

    def list_roles(self):
        return self.roles

    def gen_role_config(self, fr_role):
        old_roles = []
        old_roles_dict = {}
        functional_roles = []
        cursor = self.sf_conn.run_query(f"show grants to role {fr_role}")
        for row in cursor:
            if row[1] != 'OWNERSHIP':
                if row[1] == 'USAGE':
                    match_type = re.search(r'_(\w{2,3})_AR$', row[3])
                    if match_type:
                        old_roles.append(row[3])
                        old_roles_dict[row[3]] = 1
                    match_type = re.search(r'_FR$', row[3])
                    if match_type:
                        functional_roles.append(row[3])
                else:
                    # Not _ARs or _FRs 
                    # How can we fix this?
                    print("Error cannot generate configuration for row: ", row)
        union_arr = []
        for role in old_roles:
            union_arr.append(f"'{role}'")
        union_sql = ",".join(union_arr)
    
        wh_roles = {}
        cursor = self.sf_conn.run_query(f"select distinct grantee_name, wh_name\n  from allgrantswh where grantee_name in ({union_sql}) order by wh_name asc")
        for row in cursor:
            wh_roles[row[0]] = row[1]
            old_roles_dict[row[0]] = 0
        sc_roles = {}
        cursor = self.sf_conn.run_query(f"select distinct grantee_name, db_name, sc_name, db_sc\n  from allfuturegrantssc where grantee_name in ({union_sql}) order by db_sc asc")
        for row in cursor:
            sc_roles[row[0]] = row
            old_roles_dict[row[0]] = 0
        db_roles = {}
        cursor = self.sf_conn.run_query(f"select * from allfuturegrantsdb where grantee_name in ({union_sql}) order by db_name asc")
        for row in cursor:
            db_roles[row[0]] = row
            old_roles_dict[row[0]] = 0

        # Remaining roles in old_roles_dict where the value is still 1
        custom_includes = []

        for c_role in old_roles_dict:
            if old_roles_dict[c_role] == 1:
                custom_includes.append(f"\"{c_role}\"")

        custom_include = ", ".join(custom_includes)
            
        includes = []

        for wh_r in wh_roles:
            match_type = re.search(r'_(\w{2,3})_AR$', wh_r)
            if match_type:
                type = match_type.group(1)
                includes.append("      {" + f" \"TYPE\" : \"WAREHOUSE\", \"WAREHOUSE\" : \"{wh_roles[wh_r]}\", \"ROLE\" : \"{type}\"" + " }")

        for sc_r in sc_roles:
            match_type = re.search(r'_(\w{2,3})_AR$', sc_r)
            if match_type:
                type = match_type.group(1)
                includes.append("      {" + f" \"TYPE\" : \"SCHEMA\", \"DATABASE\" : \"{sc_roles[sc_r][1]}\", \"SCHEMA\" : \"{sc_roles[sc_r][2]}\", \"ROLE\" : \"{type}\"" + " }")

        for db_r in db_roles:
            match_type = re.search(r'_(\w{2,3})_AR$', db_r)
            if match_type:
                type = match_type.group(1)
                includes.append("      {" + f" \"TYPE\" : \"DATABASE\", \"DATABASE\" : \"{sc_roles[sc_r][1]}\", \"ROLE\" : \"{type}\"" + " }")

        for fr_r in functional_roles:
            includes.append("      {" + f" \"TYPE\" : \"ROLE\", \"ROLE\" : \"{fr_r}\"" + " }")
            
        include = ",\n".join(includes)

        config = []
        config.append(f"  \"{fr_role}\" : " + "{")
        config.append( "    \"ORDER\" : \"INCEXC\",")
        config.append( "    \"INCLUDE\" : [")
        config.append(include)
        config.append( "    ],")
        config.append( "    \"EXCLUDE\" : [],")
        config.append(f"    \"CUSTOM_INCLUDE\" : [ {custom_include} ],")
        config.append( "    \"CUSTOM_EXCLUDE\" : [],")
        config.append( "    \"SCIM_ROLES\" : []")
        config.append( "  }")
        self.generated_config.append("\n".join(config))

    def config(self):
        role_config = "\n".join(self.generated_config)
        return "{\n" + role_config + "\n}"

    def print_config(self):
        role_config = self.config()
        print(role_config)
        
