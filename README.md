# snowflake-provisioning

Snowflake Database, Schema, and Warehouse provisioning

## Overivew

Python based Snowflake Database/Schema/Warehouse Provisioning script that provides a frame-work for deploying objects with configurable and customizable access roles.

<img src="images/Diagram1.png" alt="Provisioning of Database, Schema, and Warehouse Objects" title="Provisioning Objects" /> 

Inheritance between access roles is setup based on configurable parameters and is managed between database and schemas as a simplified illustration below shows:

<img src="images/Diagram2.png" alt="Provisioned Access Role Hierarchy between Database, Schema, and warehouse" title="Access Role Hierarchy" />

The provisioned database/schema/warehouse and access roles allow for an easy way of setting Automated Governance of Role-Based Access Control.

It can of course also be managed manually creating roles and granting access directly to a role like:

```
CREATE ROLE IF NOT EXISTS TEST_READER_FR;
GRANT USAGE ON ROLE TEST_DB_TEST_SC_RO_AR TO ROLE TEST_READER_FR;
GRANT USAGE ON ROLE TEST_WH_USE_AR        TO ROLE TEST_READER_FR;
```

Leading to the following role hierarchy:

<img src="images/Diagram3.png" alt="Functional role and the hierarchy of grants it gives access to" title="Functional role hierarchy" />

With this flexible tool you can embed environment names in your naming convention like including PROD/TEST/DEV in your name. An example could be =PROD_TEST_DB=. Now you can rely on your Role-Based Acces Control only having access to PROD roles by limiting to roles with PROD in the beginning of the name.

## Table of Contents

1. [Overview](#overview)
1. [Configuration](#configuration)
1. [Executing](#executing)
1. [Automating](#automating_functional_roles)
1. [Deficiencies](#deficiencies)
1. [Author](#author)
1. [Credits](#credits)
1. [License](#license)

## Configuration

3 configuration files are needed and a sample set of configurations are provided. Each of the configuration files are json-formatted. 

The configuration of the provision tool allows you to create multiple different types of access roles. If you want to customize a special role that allows it to execute stored procedures and tasks, but not directly write to tables that is possible as long as you can specify it in Snowflake grant terms. 

## Executing

Embedded help is provided with the scripts:

```$ ./sf_create_obj --help
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

Each object has its own options reflecting all the parameters for the given snowflake object:

```$ ./sf_create_obj warehouse --help
usage: sf_create_obj warehouse [-h] [--warehouse_size WAREHOUSE_SIZE] [--max_cluster_count MAX_CLUSTER_COUNT] [--min_cluster_count MIN_CLUSTER_COUNT]
                               [--scaling_policy {STANDARD,ECONOMY}] [--auto_suspend AUTO_SUSPEND] [--auto_resume] [--initially_suspended] [--resource_monitor RESOURCE_MONITOR]
                               [--enable_query_acceleration] [--query_acceleration_max_scale_factor QUERY_ACCELERATION_MAX_SCALE_FACTOR]
                               [--max_concurrency_level MAX_CONCURRENCY_LEVEL] [--statement_queued_timeout_in_seconds STATEMENT_QUEUED_TIMEOUT_IN_SECONDS]
                               [--statement_timeout_in_seconds STATEMENT_TIMEOUT_IN_SECONDS] [--comment COMMENT] [--tag TAG]
                               name

positional arguments:
  name                  Name of Snowflake object to provision

optional arguments:
  -h, --help            show this help message and exit
  --warehouse_size WAREHOUSE_SIZE
                        Warehouse Size: XSmall - X6Large
  --max_cluster_count MAX_CLUSTER_COUNT
                        Warehouse Max Cluster Count 1 for standard edition, 1-10 for higher editions
  --min_cluster_count MIN_CLUSTER_COUNT
                        Warehouse Min Cluster Count 1 for standard edition, 1-10 for higher editions
  --scaling_policy {STANDARD,ECONOMY}
                        Warehouse Scaling Policy: STANDARD or ECONOMY
  --auto_suspend AUTO_SUSPEND
                        Warehouse Auto-Suspend: Minimum 60 seconds, 0 to never suspend
  --auto_resume         Warehouse Auto-Resume: FALSE by including this parameter TRUE by not including it
  --initially_suspended
                        Warehouse Initially Suspended: FALSE by including this parameter TRUE by not including it
  --resource_monitor RESOURCE_MONITOR
                        Name of resource monitor to add warehouse to
  --enable_query_acceleration
                        Enable query acceleration: FALSE by including this parameter TRUE by not including it
  --query_acceleration_max_scale_factor QUERY_ACCELERATION_MAX_SCALE_FACTOR
                        Query acceleration max scale factorn: 0-100, default: 8
  --max_concurrency_level MAX_CONCURRENCY_LEVEL
                        Max concurrency level
  --statement_queued_timeout_in_seconds STATEMENT_QUEUED_TIMEOUT_IN_SECONDS
                        Statement Queued Timeout in Seconds
  --statement_timeout_in_seconds STATEMENT_TIMEOUT_IN_SECONDS
                        Statement Timeout in Seconds
  --comment COMMENT     Comment to add to object
  --tag TAG             Add a single tag_name=value to object
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
This starts becoming a problem when you have too many objects deployed and will need a simple cache of future grants updated everytime new databases, schemas, or warehouses are provisioned/deprovisioned.

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
