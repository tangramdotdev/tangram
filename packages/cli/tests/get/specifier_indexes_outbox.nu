use ../../test.nu *

# Getting a specifier waits for a pending database index batch.

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
let producer = server spawn --name producer --directory $directory --config {
	roles: [http runner scheduler]
}
let group = tg --url $producer.url group create project | from json
stop $producer

# A get by specifier asks the indexer to catch up before retrying the index.
let indexer = server spawn --name indexer --directory $directory
let output = tg --url $indexer.url get project | from json
assert equal $output.id $group.id
