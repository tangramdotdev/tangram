dropdb -U postgres -h localhost database
dropdb -U postgres -h localhost index
nats consumer rm -f index index
nats stream rm -f index
cqlsh -e 'drop keyspace store;'
