cockroach sql --insecure --host=localhost:26257 -e 'create database database'
cockroach sql --insecure --host=localhost:26257 -d database -f packages/server/src/database/postgres.sql

createdb -U postgres -h localhost sandbox_store
psql -U postgres -h localhost -d sandbox_store -f packages/server/src/sandbox_store/postgres.sql

nats stream create processes_finalize_queue --discard new --retention work --subjects processes.finalize.queue --defaults
nats stream create processes_signals --discard new --retention work --subjects processes.*.signal --defaults
nats stream create processes_stdio --discard new --retention work --subjects processes.stdio.*.* --defaults

cqlsh -e r#'create keyspace store with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
cqlsh -k store -f packages/store/src/scylla.cql
