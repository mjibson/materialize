# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Prime the Cockroach database
#

$ postgres-execute connection=postgres://root@crdb:26257

DROP DATABASE IF EXISTS defaultdb;
CREATE DATABASE defaultdb;

SET CLUSTER SETTING kv.rangefeed.enabled = true;

CREATE TABLE pk_table (i INT8 PRIMARY KEY, j INT8);
INSERT INTO pk_table VALUES (1, 5);

#
# Error checking
#

> CREATE MATERIALIZED SOURCE "no_such_host"
  FROM COCKROACH HOST 'host=no_such_postgres.mtrlz.com port=26257 user=root'
  TABLE 'pk_table' (f1 TIMESTAMP, f2 TEXT);

! SELECT * FROM no_such_host;
Source error: File IO: error connecting to server: failed to lookup address information: Name or service not known

> CREATE MATERIALIZED SOURCE "no_such_port"
  FROM COCKROACH HOST 'host=crdb port=65534 user=root'
  TABLE 'pk_table' (f1 TIMESTAMP, f2 TEXT);

! SELECT * FROM no_such_port;
Source error: File IO: error connecting to server: Connection refused (os error 111)

> CREATE MATERIALIZED SOURCE "no_such_user"
  FROM COCKROACH HOST 'host=crdb port=26257 user=no_such_user password=password'
  TABLE 'pk_table' (f1 TIMESTAMP, f2 TEXT);

! SELECT * FROM no_such_user;
Source error: File IO: db error: ERROR: password authentication failed for user no_such_user

> CREATE MATERIALIZED SOURCE "no_such_table"
  FROM COCKROACH HOST 'host=crdb port=26257 user=root'
  TABLE 'no_such_table' (pk INTEGER, f2 TEXT);

! SELECT * FROM no_such_table;
Source error: File IO: db error: ERROR: failed to resolve targets in the CHANGEFEED stmt: table "no_such_table" does not exist

#
# Establish direct replication
#

> CREATE MATERIALIZED SOURCE "pk_table"
  FROM COCKROACH HOST 'host=crdb port=26257 user=root'
  TABLE 'pk_table' (pk INTEGER, f2 TEXT);

#
# Perform sanity checks of the initial snapshot
#

> SELECT * FROM pk_table;
1 5

#
# Modify the tables on the Cockroach side
#

$ postgres-execute connection=postgres://root@crdb:26257
INSERT INTO pk_table VALUES (2, 6);
INSERT INTO pk_table VALUES (3, 7);
DELETE FROM pk_table WHERE i = 1;
UPDATE pk_table SET j = '4' WHERE i = 2;
UPDATE pk_table SET i = i + 10 WHERE i BETWEEN 3 AND 4;

#
# Check the updated data on the Materialize side
#

> SELECT * FROM pk_table;
13 7
2 4

#
# Remove all data on the Postgres side
#

$ postgres-execute connection=postgres://root@crdb:26257
DELETE FROM pk_table;

#
# Check that all data sources empty out on the Materialize side
#

> SELECT COUNT(*) = 0 FROM pk_table;
true

$ postgres-execute connection=postgres://root@crdb:26257
TRUNCATE pk_table

! SELECT COUNT(*) = 0 FROM pk_table;
Source error: File IO: db error: ERROR: "pk_table" was truncated
