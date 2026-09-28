use ../lib/test.nu *

# Getting a specifier waits for a pending database index batch.

def stop [instance: record] {
	let pid = open ($instance.directory | path join 'lock') | into int
	kill --signal 2 $pid
	if $nu.os-info.name == 'linux' {
		^tail --pid $pid -f /dev/null
	} else {
		while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
	}
}

let directory = mktemp -d

# Commit a database mutation with the indexer disabled.
let local_producer = server spawn --name local-producer --directory $directory --config {
	roles: [api runner scheduler]
}
let group = tg --url $local_producer.url group create project | from json
stop $local_producer

# A get by specifier asks the indexer to catch up before retrying the index.
let local_indexer = server spawn --name local-indexer --directory $directory
let output = tg --url $local_indexer.url get project | from json
assert equal $output.id $group.id
