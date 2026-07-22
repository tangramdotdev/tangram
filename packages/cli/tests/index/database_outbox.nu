use ../../test.nu *

# A database index batch is committed durably and serviced by a later indexer.

def stop [server: record] {
	let pid = open ($server.directory | path join 'lock') | into int
	kill --signal 2 $pid
	if $nu.os-info.name == 'linux' {
		^tail --pid $pid -f /dev/null
	} else {
		while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
	}
}

let directory = mktemp -d

# Commit a database mutation with the indexer disabled.
let producer = spawn --name producer --directory $directory --config { indexer: false }
let group = tg --url $producer.url group create project | from json
stop $producer

# A later indexer services the durable intent, and tg index waits for it.
let indexer = spawn --name indexer --directory $directory
tg --url $indexer.url index
let indexed = tg --url $indexer.url group get project | from json
assert equal $indexed.id $group.id
