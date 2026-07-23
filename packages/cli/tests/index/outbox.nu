use ../../test.nu *

# An index batch is durably enqueued by one server and serviced by a later indexer.

let directory = mktemp -d
let producer = spawn --name producer --directory $directory --config {
	advanced: { single_process: false },
	roles: [cleaner finalizer http runner scheduler watchdog],
}

let id = tg --url $producer.url put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim

let pid = open ($producer.directory | path join 'lock') | into int
kill --signal 2 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}

let indexer = spawn --name indexer --directory $directory --config {
	advanced: { single_process: false },
}

tg --url $indexer.url index
let metadata = tg --url $indexer.url object metadata $id | from json
assert equal $metadata.subtree.count 5 "the object tree should be indexed by the second server"
