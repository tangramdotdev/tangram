cockroach sql --insecure --host=localhost:26257 -e 'create database database'
cockroach sql --insecure --host=localhost:26257 -d database -f packages/server/src/database/postgres.sql

createdb -U postgres -h localhost processes
psql -U postgres -h localhost -d processes -f packages/server/src/process/store/postgres.sql

cqlsh -e r#'create keyspace objects with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
cqlsh -k objects -f packages/stores/object/src/scylla.cql
