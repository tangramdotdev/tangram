cockroach sql --insecure --host=localhost:26257 -e 'create database database'
cockroach sql --insecure --host=localhost:26257 -d database -f packages/server/src/database/postgres.sql

createdb -U postgres -h localhost process_store
psql -U postgres -h localhost -d process_store -f packages/server/src/process/store/postgres.sql

cqlsh -e r#'create keyspace object_store with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
cqlsh -k object_store -f packages/stores/object/src/scylla.cql
