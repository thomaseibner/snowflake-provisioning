# snowflake-provisioning

Snowflake Database, Schema, and Warehouse provisioning

## Table of Contents

1. [Overview](#overview)
   1. [Functional and Access roles](#functional-and-access-roles)
1. [Configuration](#configuration)
1. [Executing](#executing)
   1. [Creating and Dropping Warehouses](#creating-and-dropping-warehouses)
1. [Automating](#automating_functional_roles)
1. [Deficiencies](#deficiencies)
1. [Author](#author)
1. [Credits](#credits)
1. [License](#license)

## Overivew

This repository contains a Python-based Snowflake Database/Schema/Warehouse Provisioning script, that provides a
framework for deploying objects with configurable and customizable access roles.
![Provisioning of Database, Schema, and Warehouse Objects](images/Diagram1.png)
Snowflake supports a highly granular and configurable RBAC[^1] (Role-Based Access Control) scheme.
The posibilities of configuration are endless and it is almost too complex to understand when you are new to Snowflake.
`sf_create_obj` aims to provide an example of an implementation that can be easily extended while showing all sql
commands used to build out the framework with. This also allows for easy integration with
[schemachange](https://github.com/Snowflake-Labs/schemachange)/
[Snowchange](https://jeremiahhansen.medium.com/snowchange-a-database-change-management-tool-b9f0b786a7da) as both deploy
and rollback output is provided.

### Functional and Access roles

Best practice as recommended by Snowflake is to separate functional access from the underlying object access, i.e.,
database, schema, individual tables, etc.

Functional access can be defined as a role that a script or user needs to perform and operation across many schemas or
databases. In the examples provided all functional roles have a postfix of _FR in the role name. 

Access roles govern access to a privilege on an object. An example is an access role that provides write access to all
tables in a schema. Access roles often are nested and a role at a database level can provide access to all underlying
schemas. That means rather than explicitly granting a privilege it can be inherited from another role. A common
convention that is followed here is to use a postfix of _AR to signify an access role. 

Using _AR and _FR to provide clear separation between roles makes it easier to quickly see the difference when looking
at the roles in Snowflake. Taking this one step further to simplify how your roles are shown in both Snowflake's
Classic UI as well as Snowsight this framework allows you to prefix access roles with a custom name which includes an
underscore (_). This puts the access roles at the end of the role list and helps avoid users using access roles
directly. Having a customizable prefix also allows us to think ahead to the automation of functional roles. In the
sample configuration database access roles are prefixed by \_DB\_, schema access roles with \_SC\_, and warehouses
with \_WH\_ respectively. 

Inheritance between database and schema access roles is setup based on configurable parameters and is managed
automatically between database and schemas as a simplified illustration below shows:

![Provisioned Access Role Hierarchy between Database, Schema, and warehouse](images/Diagram2.png)

The provisioned database/schema/warehouse and access roles allow for an easy way of setting Automated Governance of Role-Based Access Control.

It can of course also be managed manually creating roles and granting access directly to a role like:

```
CREATE ROLE IF NOT EXISTS TEST_READER_FR;
GRANT ROLE _SC_TEST_DB_TEST_SC_RO_AR TO ROLE TEST_READER_FR;
GRANT ROLE _WH_TEST_WH_USE_AR        TO ROLE TEST_READER_FR;
```

Leading to the following role hierarchy:

![Functional role and the hierarchy of grants it gives access to](images/Diagram3.png)

With this flexible tool you can embed environment names in your naming convention like including PROD/TEST/DEV in your name. An example could be =PROD_TEST_DB=. Now you can rely on your Role-Based Acces Control only having access to PROD roles by limiting to roles with PROD in the beginning of the name.


## Configuration

3 configuration files are needed and a sample set of configurations are provided. Each of the configuration files are json-formatted. 

The configuration of the provision tool allows you to create multiple different types of access roles. If you want to customize a special role that allows it to execute stored procedures and tasks, but not directly write to tables that is possible as long as you can specify it in Snowflake grant terms. 

## Executing

Embedded help is provided with the scripts:

```
$ ./sf_create_obj --help
usage: sf_create_obj [-h] {database,schema,warehouse} ...

Snowflake database, schema, and warehouse provisioning

positional arguments:
  {database,schema,warehouse}
                        sub-command help
    database            Provision database in Snowflake
    schema              Provision schema in Snowflake
    warehouse           Provision warehouse in Snowflake

optional arguments:
  -h, --help            show this help message and exit
```

Each object has its own options reflecting all the parameters for the given snowflake object. An example is the embedded help for creating a database:

```
$ ./sf_create_obj database --help
usage: sf_create_obj database [-h] [--data_retention_time_in_days DATA_RETENTION_TIME_IN_DAYS] [--max_data_extension_time_in_days MAX_DATA_EXTENSION_TIME_IN_DAYS] [--transient]
                              [--default_ddl_collation DEFAULT_DDL_COLLATION] [--comment COMMENT] [--tag TAG]
                              name

positional arguments:
  name                  Name of Snowflake object to provision

optional arguments:
  -h, --help            show this help message and exit
  --data_retention_time_in_days DATA_RETENTION_TIME_IN_DAYS
                        Time Travel in days 1 for standard edition 1-90 for higher editions
  --max_data_extension_time_in_days MAX_DATA_EXTENSION_TIME_IN_DAYS
                        Maximum number of days Snowflake can extend data retention period 1-90
  --transient           Applies to database and schema, creates transient object
  --default_ddl_collation DEFAULT_DDL_COLLATION
                        Default DDL Collation
  --comment COMMENT     Comment to add to object
  --tag TAG             Add a single tag_name=value to object
```

By default the script prints the DDL to STDOUT. This is to allow for pushing the create/drop (rollback) output into a snowchange/schemachange pipeline for execution. 

### Creating and Dropping Warehouses

The simplest way to show how a sample warehouse could be provisioned is to run it with the [default configuration](wh-config.json) and simply provide a name:

```
$ ./sf_create_obj warehouse TEST_WH
CREATE WAREHOUSE IF NOT EXISTS TEST_WH
  AUTO_RESUME                         = TRUE
  AUTO_SUSPEND                        = 60
  COMMENT                             = 'sf_create_obj created warehouse'
  ENABLE_QUERY_ACCELERATION           = TRUE
  INITIALLY_SUSPENDED                 = TRUE
  MAX_CLUSTER_COUNT                   = 1
  MAX_CONCURRENCY_LEVEL               = 16
  MIN_CLUSTER_COUNT                   = 1
  QUERY_ACCELERATION_MAX_SCALE_FACTOR = 16
  SCALING_POLICY                      = STANDARD
  STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 1800
  STATEMENT_TIMEOUT_IN_SECONDS        = 3600
;
CREATE ROLE IF NOT EXISTS TEST_WH_USE_AR;
CREATE ROLE IF NOT EXISTS TEST_WH_MON_AR;
CREATE ROLE IF NOT EXISTS TEST_WH_MOD_AR;
CREATE ROLE IF NOT EXISTS TEST_WH_ADM_AR;

GRANT OWNERSHIP ON WAREHOUSE TEST_WH TO ROLE SYSADMIN REVOKE CURRENT GRANTS;
GRANT ALL PRIVILEGES ON WAREHOUSE TEST_WH TO ROLE SYSADMIN;
GRANT OWNERSHIP ON ROLE TEST_WH_USE_AR TO ROLE SYSADMIN REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON ROLE TEST_WH_MON_AR TO ROLE SYSADMIN REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON ROLE TEST_WH_MOD_AR TO ROLE SYSADMIN REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON ROLE TEST_WH_ADM_AR TO ROLE SYSADMIN REVOKE CURRENT GRANTS;

GRANT ROLE TEST_WH_USE_AR TO ROLE TEST_WH_MON_AR;
GRANT ROLE TEST_WH_MON_AR TO ROLE TEST_WH_MOD_AR;
GRANT ROLE TEST_WH_MOD_AR TO ROLE TEST_WH_ADM_AR;
GRANT ROLE TEST_WH_ADM_AR TO ROLE SYSADMIN;

GRANT USAGE, OPERATE ON WAREHOUSE TEST_WH TO ROLE TEST_WH_USE_AR;
GRANT MONITOR ON WAREHOUSE TEST_WH TO ROLE TEST_WH_MON_AR;
GRANT MODIFY ON WAREHOUSE TEST_WH TO ROLE TEST_WH_MOD_AR;
GRANT ALL ON WAREHOUSE TEST_WH TO ROLE TEST_WH_ADM_AR;
```
With an easy way to drop the warehouse, roles, and associated grants:
```
$ ./sf_drop_obj warehouse TEST_WH
REVOKE ALL ON WAREHOUSE TEST_WH FROM ROLE TEST_WH_ADM_AR;
REVOKE MODIFY ON WAREHOUSE TEST_WH FROM ROLE TEST_WH_MOD_AR;
REVOKE MONITOR ON WAREHOUSE TEST_WH FROM ROLE TEST_WH_MON_AR;
REVOKE USAGE, OPERATE ON WAREHOUSE TEST_WH FROM ROLE TEST_WH_USE_AR;

REVOKE ROLE TEST_WH_ADM_AR FROM ROLE SYSADMIN;
REVOKE ROLE TEST_WH_MOD_AR FROM ROLE TEST_WH_ADM_AR;
REVOKE ROLE TEST_WH_MON_AR FROM ROLE TEST_WH_MOD_AR;
REVOKE ROLE TEST_WH_USE_AR FROM ROLE TEST_WH_MON_AR;

DROP ROLE IF EXISTS TEST_WH_ADM_AR;
DROP ROLE IF EXISTS TEST_WH_MOD_AR;
DROP ROLE IF EXISTS TEST_WH_MON_AR;
DROP ROLE IF EXISTS TEST_WH_USE_AR;
DROP WAREHOUSE IF EXISTS TEST_WH;
```



The object name is validated against the allowed object names in Snowflake. 

## Automating Functional Roles

Building an automatic tool to provision and maintain functional roles is straight forward when you consistently deploy database and schema objects with the appropriate access roles.

```
-- Checking which access roles have been granted to a functional role:
SHOW GRANTS ON TEST_READER_FR;
-- Finding possible access roles for a given DATABASE:
SHOW FUTURE GRANTS IN DATABASE;
SHOW FUTURE GRANTS IN DATABASE.SCHEMA;
```
If your provisioning of objects is entirely governed by a the consistent naming convention `sf_create_obj` provides you can make it even simpler based on the available access roles in the account:

```
show roles like '_%_AR'
parse output
for each role you care about:
  show grants to role
  add/remove grants needed

```


## Deficiencies 

Currently does not support "GRANT ALL ON ALL PIPES IN SCHEMA", tags, search optimizations.
Currently does not support quoted object names in Snowflake.

## Author

Thomas Eibner (@thomaseibner) [twitter](http://twitter.com/thomaseibner) [LinkedIn](https://www.linkedin.com/in/thomaseibner/)

## Credits

Scott Redding @ Snowflake for great conversations on the topic and always being willing to hear out ideas.

Ryan Wieber @ Snowflake likewise for entertaining my questions and having a lot of patience. 

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this tool except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.




[^1]: https://docs.snowflake.com/en/user-guide/security-access-control-overview.html
