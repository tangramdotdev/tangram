do --capture-errors { cargo build --package tangram_scylla_client }
let target_directory = cargo metadata --format-version 1 --no-deps | from json | get target_directory
let scylla_client_path = $target_directory | path join debug tangram_scylla_client

createdb -U postgres -h localhost database
psql -U postgres -h localhost -d database -v ON_ERROR_STOP=1 -f packages/server/src/database/postgres.sql
createdb -U postgres -h localhost processes

^$scylla_client_path -e r#'create keyspace store with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };'#
^$scylla_client_path -k store -f packages/store/src/scylla.cql
