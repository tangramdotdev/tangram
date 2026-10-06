use ../lib/test.nu *

# Start the queue and indexer waits together, without waiting for log cleanup.
let local = server spawn --config {
	advanced: { checkpoints: true },
	indexer: { request: { poll_interval: 0.01 } },
}
let waits = [indexer.request.wait index.wait.database_index_queue]
let watches = $waits | each {|name|
	let watch = tg --url $local.url checkpoint watch $name | from json | get watch
	{ name: $name, watch: $watch }
}
let request = job spawn {
	let job_id = job id
	let output = tg --url $local.url index | complete
	$output | job send --tag $job_id 0
}
for watch in ($watches | first 2) {
	tg --url $local.url checkpoint wait $watch.name $watch.watch 0 | ignore
}
for watch in ($watches | first 2) {
	tg --url $local.url checkpoint unwatch $watch.name $watch.watch
}
let output = job recv --tag $request --timeout 10sec
success $output
