use ../../util/test.nu *

# A child process consumes a sibling's output while the runner that produced it is still pushing it.
# The parent runs on one runner and a first child borrows its capacity there, so the producer and the
# consumer run on the other two runners. The consumer's command references the producer's output by
# a referent that carries the authorization token for the producer's sync, so the parent's runner pushes the command without
# the output, and the consumer waits for the producer's push to complete instead of failing.

let root_token = random chars

# The remote stores one object per batch so that every object before the held blob is stored.
let store = { object_max_batch: 1 }
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
	sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
}

# Create three runners with capacity for one process each.
let runners = 1..3 | each {|index|
	let created = tg --url $remote.url --token $root_token runner create | from json
	let runner = server spawn --name $"runner-($index)" --config {
		advanced: { checkpoints: true },
		remotes: { default: { token: $created.token.token, url: $remote.url } },
		roles: [api indexer runner],

		scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
		runner: { cpus: 1, overcommit: { cpu: 1 }, id: $created.data.id, remote: "default", token: $created.token.token },
	}
	{ index: $index, url: $runner.url }
}

let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# The parent starts a child that holds its runner's capacity, builds the producer, then passes the
# producer's output to the consumer.
let path = artifact {
	tangram.ts: '
		export default async function () {
			let stealer = await tg.build(steal).spawn();
			let first = await tg.build(one);
			let second = await tg.build(two, first);
			await stealer.cancel();
			return second;
		}
		export async function steal() {
			await tg.sleep(60);
		}
		export function one() {
			return tg.file("first");
		}
		export async function two(file: tg.File) {
			let text = await file.text;
			return tg.file(`${text} second`);
		}
	'
}
let blob = tg --url $local.url put --no-tokens 'tg.blob("first")' | referent node

# Record which runner starts each process.
let start_tag = random int 1000000..2000000
def record_starts [runner: record, tag: int] {
	let watch = (
		tg --url $runner.url checkpoint watch runner.process.start
		| from json
		| get watch
	)
	job spawn {
		mut hit = 0
		loop {
			let output = tg --url $runner.url checkpoint wait runner.process.start $watch $hit | complete
			if $output.exit_code != 0 {
				break
			}
			let params = $output.stdout | from json | get params
			tg --url $runner.url checkpoint continue runner.process.start $watch $hit
			{ index: $runner.index, process: $params.process } | job send --tag $tag 0
			$hit += 1
		}
	} | ignore
	{ url: $runner.url, watch: $watch }
}
let recorders = $runners | each {|runner| record_starts $runner $start_tag }

# Hold every runner's capacity release so the producer's runner stays busy after the producer finishes and the consumer lands on the third runner.
let capacity_watches = $runners | each {|runner|
	let watch = (
		tg --url $runner.url checkpoint watch runner.sandbox.capacity.release
		| from json
		| get watch
	)
	{ runner: $runner, watch: $watch }
}

# Hold the producer's output blob on the remote so that its push stays open.
let blob_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json)
	| from json
	| get watch
)

# Watch the remote's store read of the blob so the test can prove the consumer's pull reaches the held object.
let read_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.put.store.object --params ({ id: $blob } | to json)
	| from json
	| get watch
)

# Release the read immediately because proven permissions can let it precede the storage request.
let read = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $root_token checkpoint wait --timeout 60s sync.put.store.object $read_watch 0 | complete
	tg --url $remote.url --token $root_token checkpoint unwatch sync.put.store.object $read_watch
	$output | job send --tag $job_id 0
}

# Watch the control acknowledgement so the test can prove the consumer waits on the producer's sync.
let ack_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.service.ack --params ({ node: $blob } | to json)
	| from json
	| get watch
)

# Start the build and wait for the producer's push to reach the blob.
let parent = tg --url $local.url build --no-tokens --remote --detach $path | referent node
let wait = job spawn {
	let job_id = job id
	let output = tg --url $local.url wait $parent | complete
	$output | job send --tag $job_id 0
}
let output = tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.get.store.object $blob_watch 0 | complete
success $output "the producer's push should reach its output blob"

# The consumer asks the producer's sync to notify it when the blob is stored.
let output = tg --url $remote.url --token $root_token checkpoint wait --timeout 20s sync.service.ack $ack_watch 0 | complete
success $output "the producer's sync should acknowledge the consumer's request"
tg --url $remote.url --token $root_token checkpoint unwatch sync.service.ack $ack_watch

# The build waits for the blob rather than failing.
let output = try { job recv --tag $wait --timeout 0sec } catch { null }
if $output != null {
	error make { msg: $"the build should wait while the push is held: ($output)" }
}

# Release the blob. The consumer's pull completes once the producer's push completes.
tg --url $remote.url --token $root_token checkpoint continue sync.get.store.object $blob_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $blob_watch

# The consumer reached the remote's store read.
let output = job recv --tag $read --timeout 20sec
success $output "the consumer should pull the blob from the remote"

# The build completes with the consumer's output.
let output = job recv --tag $wait --timeout 60sec
success $output "the build should complete after the push finishes"
let file = $output.stdout | from json | get output.value
let output = tg --url $local.url read $file | complete
success $output "the consumer's output should be readable"
snapshot ($output.stdout | str trim) 'first second'
for entry in $capacity_watches {
	tg --url $entry.runner.url checkpoint unwatch runner.sandbox.capacity.release $entry.watch
}

# The stealer ran on the parent's runner, and the producer and the consumer ran on the other two.
let children = tg --url $local.url get $parent | from json | get children | each {|child| $child.process | split row '?' | first }
let starts = 0..3 | each { job recv --tag $start_tag --timeout 30sec }
for recorder in $recorders {
	tg --url $recorder.url checkpoint unwatch runner.process.start $recorder.watch
}
def runner_of [starts: list, process: string] {
	$starts | where process == $process | get index | first
}
let parent_runner = runner_of $starts $parent
let stealer_runner = runner_of $starts ($children | get 0)
let producer_runner = runner_of $starts ($children | get 1)
let consumer_runner = runner_of $starts ($children | get 2)
assert equal $stealer_runner $parent_runner "the stealer should run on the parent's runner"
assert ($producer_runner != $parent_runner) "the producer should run on another runner"
assert ($consumer_runner != $parent_runner) "the consumer should run on another runner"
assert ($consumer_runner != $producer_runner) "the consumer should not run on the producer's runner"
