use ../../test.nu *

# An index batch survives an indexer crash and is recovered on restart.

let directory = mktemp -d
let config = {
	advanced: { checkpoints: true, single_process: false },
	indexer: { id: 'idx_0000000000000000000000000000' },
}
let server = server spawn --name server --directory $directory --config $config
let watch = tg --url $server.url checkpoint watch index.batch | from json | get watch

let id = tg --url $server.url put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --url $server.url checkpoint wait index.batch $watch 0 | ignore

let pid = open ($server.directory | path join 'lock') | into int
kill --signal 9 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}

let server = server start $server

tg --url $server.url index
let metadata = tg --url $server.url object metadata $id | from json
assert equal $metadata.subtree.count 5 "the object tree should be indexed after recovery"
