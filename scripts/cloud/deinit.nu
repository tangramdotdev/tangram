dropdb -U postgres -h localhost database | ignore

dropdb -U postgres -h localhost index | ignore

nats stream rm -f index | ignore

nats stream rm -f finish | ignore

cqlsh -e 'drop keyspace store;' | ignore
