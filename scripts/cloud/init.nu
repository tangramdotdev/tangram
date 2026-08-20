createdb -U postgres -h localhost database
psql -U postgres -h localhost -d database -v ON_ERROR_STOP=1 -f packages/server/src/database/postgres.sql
createdb -U postgres -h localhost processes

cqlsh -e r#'create keyspace objects with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
cqlsh -k objects -f packages/stores/object/src/scylla.cql
