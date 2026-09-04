use ../../test.nu *

# A separately written object grant remains durable when indexing is interrupted.

let directory = mktemp -d
let root_token = random chars
let config = {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	indexer: { id: 'idx_0000000000000000000000000000' },
}
let producer = server spawn --name producer --directory $directory --config $config
let watch = tg --url $producer.url --token $root_token checkpoint watch index.batch | from json | get watch

let directory_id = tg --url $producer.url --token $root_token put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --url $producer.url --token $root_token grant public object_subtree $directory_id | ignore
tg --url $producer.url --token $root_token checkpoint wait index.batch $watch 0 | ignore

let pid = open ($producer.directory | path join 'lock') | into int
kill --signal 9 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}

let indexer = server start $producer
tg --url $indexer.url --token $root_token index
let local = server spawn --name local --config {
	remotes: { default: { url: $indexer.url } },
}

let output = tg --url $local.url --no-quiet pull $directory_id | complete
success $output "An anonymous client should pull the public directory after interrupted indexing."
