createdb -U postgres -h localhost database
psql -U postgres -h localhost -d database -f packages/server/src/database/postgres.sql

nats stream create processes.finalize.queue --retention work --subjects processes.finalize.queue --defaults
nats stream create sandboxes.queue --discard new --retention work --subjects sandboxes.queue --defaults
nats stream create sandboxes.processes.queue --discard new --retention work --subjects sandboxes.*.processes.queue --defaults
nats stream create processes.stdio --discard new --retention work --subjects processes.stdio.*.* --defaults

cqlsh -e r#'create keyspace store with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
cqlsh -k store -f packages/store/src/scylla.cql
