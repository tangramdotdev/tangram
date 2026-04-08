cockroach sql --insecure --host=localhost:26257 -e 'create database database'
cockroach sql --insecure --host=localhost:26257 -d database -f packages/server/src/database/postgres.sql

createdb -U postgres -h localhost register
psql -U postgres -h localhost -d register -f packages/server/src/register/postgres.sql

nats stream create processes_finalize_queue --discard new --retention work --subjects processes.finalize.queue --defaults
nats stream create processes_signals --discard new --retention work --subjects processes.*.signal --defaults
nats stream create processes_stdio --discard new --retention work --subjects processes.stdio.*.* --defaults
nats stream create sandboxes_processes_queue --discard new --retention work --subjects sandboxes.*.processes.queue --defaults
nats stream create sandboxes_queue --discard new --retention work --subjects sandboxes.queue --defaults

cqlsh -e r#'create keyspace store with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
cqlsh -k store -f packages/store/src/scylla.cql
