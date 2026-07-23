use ../../test.nu *

# A separately written object grant remains durable when indexing is interrupted.

let directory = mktemp -d
let producer = spawn --name producer --directory $directory --config {
	advanced: { single_process: false },
	roles: [cleaner finalizer http runner scheduler watchdog],
}

let directory_id = tg --url $producer.url put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --url $producer.url grant public object_subtree $directory_id | ignore

let pid = open ($producer.directory | path join 'lock') | into int
kill --signal 2 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}

let indexer = spawn --name indexer --directory $directory --config {
	advanced: { single_process: false },
	authentication: { users: { providers: { insecure: true } } },
}
tg --url $indexer.url index
let local = spawn --name local --config {
	remotes: { default: { url: $indexer.url } },
}

let output = tg --url $local.url --no-quiet pull $directory_id | complete
success $output "An anonymous client should pull the public directory after interrupted indexing."
