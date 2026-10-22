use ../lib/test.nu *

# Index updates committed without an indexer are applied in commit order by a later indexer.

def stop [instance: record] {
	let pid = open ($instance.directory | path join 'lock') | into int
	kill --signal 2 $pid
	wait_for_exit $pid
}

let config = {
	authentication: { users: { providers: { insecure: true } } }
}
let directory = mktemp -d

# Seed the index so authorization does not depend on the mutations under test.
let local_seed = server spawn --name local-seed --directory $directory --config $config
let alice = tg --url $local_seed.url login --verbose --name alice | from json
let bob = tg --url $local_seed.url login --verbose --name bob | from json
tg --url $local_seed.url --token $alice.token group create project | ignore
tg --url $local_seed.url index
stop $local_seed

# Commit an update followed by a delete with the indexer disabled.
let producer_config = $config | merge { roles: [api runner scheduler] }
let local_producer = server spawn --name local-producer --directory $directory --config $producer_config
tg --url $local_producer.url --token $alice.token grant $bob.user.id read project | ignore
tg --url $local_producer.url --token $alice.token revoke $bob.user.id read project | ignore
stop $local_producer

# A later indexer applies the update before the delete.
let local_indexer = server spawn --name local-indexer --directory $directory --config $config
tg --url $local_indexer.url index
failure (tg --url $local_indexer.url --token $bob.token group get project | complete)
