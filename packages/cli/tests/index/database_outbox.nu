use ../../test.nu *

# Database index batches follow commit order and are serviced by a later indexer.

def latest_batch [directory: string] {
	open ($directory | path join database)
	| query db 'select max(batch) as batch from outbox'
	| get batch.0
}

def stop [server: record] {
	let pid = open ($server.directory | path join 'lock') | into int
	kill --signal 2 $pid
	if $nu.os-info.name == 'linux' {
		^tail --pid $pid -f /dev/null
	} else {
		while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
	}
}

let config = {
	authentication: { users: { providers: { insecure: true } } }
}
let directory = mktemp -d

# Seed the index so authorization does not depend on the mutations under test.
let seed = server spawn --name seed --directory $directory --config $config
let alice = tg --url $seed.url login --verbose --name alice | from json
let bob = tg --url $seed.url login --verbose --name bob | from json
tg --url $seed.url --token $alice.token group create project | ignore
tg --url $seed.url index
stop $seed

# Commit an update followed by a delete with the indexer disabled.
let producer_config = $config | merge { roles: [cleaner http runner scheduler] }
let producer = server spawn --name producer --directory $directory --config $producer_config
tg --url $producer.url --token $alice.token grant $bob.user.id read project | ignore
let put_batch = latest_batch $directory
tg --url $producer.url --token $alice.token revoke $bob.user.id read project | ignore
let delete_batch = latest_batch $directory

assert equal $delete_batch ($put_batch + 1)
let next = (
	open ($directory | path join database)
	| query db 'select next from outbox_batch'
	| get next.0
)
assert equal $next $delete_batch
stop $producer

# A later indexer applies the update before the delete.
let indexer = server spawn --name indexer --directory $directory --config $config
tg --url $indexer.url index
failure (tg --url $indexer.url --token $bob.token group get project | complete)
