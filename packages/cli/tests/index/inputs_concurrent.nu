use ../../test.nu *

# Start all three input waits before allowing any of them to finish.
let server = server spawn --config {
	advanced: { checkpoints: true },
	indexer: { request: { poll_interval: 0.01 } },
}
let waits = [indexer.request.wait index.wait.database_index_queue index.wait.compactions]
let watches = $waits | each {|name|
	let watch = tg --url $server.url checkpoint watch $name | from json | get watch
	{ name: $name, watch: $watch }
}
let request = job spawn {
	let job_id = job id
	let output = tg --url $server.url index | complete
	$output | job send --tag $job_id 0
}
for watch in $watches {
	tg --url $server.url checkpoint wait $watch.name $watch.watch 0 | ignore
}
for watch in $watches {
	tg --url $server.url checkpoint unwatch $watch.name $watch.watch
}
let output = job recv --tag $request --timeout 10sec
success $output
