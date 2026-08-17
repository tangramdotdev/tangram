use ../../test.nu *

# A checkin reusing a watcher waits for the checkin that published its graph to finish indexing.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

let path = artifact {
	tangram.ts: 'export default "indexed";'
}

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --no-checkout-pointers --no-lock | complete
		$output | job send --tag $job_id 0
	}
}

def update_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin $path --watch --no-checkout-pointers --no-lock --update unused | complete
		$output | job send --tag $job_id 0
	}
}

# Hold the first checkin after it publishes the watcher but before it indexes the graph.
let index_watch = (
	tg checkpoint watch checkin.index --params '{"updates":""}'
	| from json
	| get watch
)
let first = checkin_background $path
tg checkpoint wait checkin.index $index_watch 0 | ignore

# A distinct checkin task must wait for the watch's pending index operation.
let second = update_background $path
let early = try {
	job recv --tag $second --timeout 250ms
} catch {
	null
}
assert ($early == null) "the second checkin should wait for the first checkin to finish indexing"

# Complete indexing and verify that both checkins succeed.
tg checkpoint continue checkin.index $index_watch 0
tg checkpoint unwatch checkin.index $index_watch
let first_output = job recv --tag $first --timeout 10sec
let second_output = job recv --tag $second --timeout 10sec
success $first_output
success $second_output
