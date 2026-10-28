use ../lib/test.nu *

# An indexer recovers a cloud queue whose pending fragments exceed ScyllaDB's memory limit for unpaged queries.

let local = server spawn --cloud --config {
	advanced: { checkpoints: true },
}
let watch = tg --url $local.url checkpoint watch index.batch | from json | get watch

# Put overlapping directory trees whose queued batches together exceed ScyllaDB's 100 MiB hard limit for unpaged queries.
let children = 0..<1_000 | each { |i|
	$'"($i)":' + 'tg.file("' + ($i | into string) + '")'
} | str join ','
let directories = 0..<30 | each { |i|
	$'"($i)":' + 'tg.directory({' + $'"unique_($i)":' + 'tg.file("unique"),' + $children + '})'
} | str join ','
let ids = 0..<110 | each { |i|
	let output = ('tg.directory({' + $'"unique_($i)":' + 'tg.file("unique"),' + $directories + '})') | tg --url $local.url put --no-tokens | complete
	success $output 'the directory must be stored'
	$output.stdout | referent node
}
tg --url $local.url checkpoint wait index.batch $watch 0 | ignore

# Crash the server while the queue holds every fragment, then restart it.
let pid = open ($local.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let server = server start $local

let output = tg --url $local.url index | complete
success $output 'the indexer must recover the queue after the restart'

# Verify that every complete directory tree was indexed after recovery.
for id in $ids {
	let metadata = tg --url $local.url object metadata $id | from json
	assert equal $metadata.subtree.count 60_093 'the directory tree must be indexed after recovery'
}
