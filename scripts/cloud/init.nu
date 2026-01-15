createdb -U postgres -h localhost database
psql -U postgres -h localhost -d database -f packages/server/src/database/postgres.sql

createdb -U postgres -h localhost index
for path in (glob packages/index/src/postgres/*.sql | sort) {
	psql -U postgres -h localhost -d index -f $path
}

nats stream create index --discard new --retention work --subjects index --defaults
nats consumer create index index --deliver all --max-pending 1000000 --pull --defaults

nats stream create finish --discard new --retention work --subjects finish --defaults
nats consumer create finish finish --deliver all --max-pending 1000000 --pull --defaults

cqlsh -e r#'create keyspace store with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
cqlsh -k store -f packages/store/src/scylla.cql
