dropdb -U postgres -h localhost database | ignore
dropdb -U postgres -h localhost index | ignore
nats consumer rm -f index index | ignore
nats stream rm -f index | ignore
cqlsh -e 'drop keyspace store;' | ignore
