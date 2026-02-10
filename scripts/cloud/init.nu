createdb -U postgres -h localhost database
psql -U postgres -h localhost -d database -f packages/server/src/database/postgres.sql

nats stream create finalize --discard new --retention work --subjects finalize --defaults
nats consumer create finalize finalize --deliver all --max-pending 1000000 --pull --defaults

nats stream create queue --discard new --retention work --subjects  queue --defaults
nats consumer create queue queue --deliver all --max-pending 1000000 --pull --defaults

cqlsh -e r#'create keyspace store with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
cqlsh -k store -f packages/store/src/scylla.cql
