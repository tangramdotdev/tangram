use ../../test.nu *

# A pull omits local index requests for incoming objects and processes whose requested fields are
# already available in the sync graph.

let remote = server spawn --cloud --name remote
let client = server spawn --name client --config {
	advanced: { checkpoints: true },
	remotes: { default: { url: $remote.url } },
}

# Put a directory with two branches. The deeper branch provides a checkpoint after the file's blob
# has updated the graph while still keeping the pull open.
let directory = (
	tg --url $remote.url put 'tg.directory({
		"f": tg.file("available"),
		"z": tg.directory({ "v": tg.file("later") }),
	})'
	| str trim
)
let file = tg --url $remote.url put 'tg.file("available")' | str trim
let deep_blob = tg --url $remote.url put 'tg.blob("later")' | str trim
tg --url $remote.url index

let object_filter_watch = (
	tg --url $client.url checkpoint watch sync.get.index.object.filter --params ({ id: $file } | to json)
	| from json
	| get watch
)
let object_index_watch = (
	tg --url $client.url checkpoint watch sync.get.index.object --params ({ id: $file } | to json)
	| from json
	| get watch
)
let object_input_watch = (
	tg --url $client.url checkpoint watch sync.get.input.object --params ({ id: $deep_blob } | to json)
	| from json
	| get watch
)

let object_pull = job spawn {
	let job_id = job id
	let output = tg --url $client.url pull $directory | complete
	$output | job send --tag $job_id 0
}

# Wait until the file is available and the final blob is held before letting the index task check the
# graph.
tg --url $client.url checkpoint wait sync.get.index.object.filter $object_filter_watch 0 | ignore
tg --url $client.url checkpoint wait sync.get.input.object $object_input_watch 0 | ignore
tg --url $client.url checkpoint continue sync.get.index.object.filter $object_filter_watch 0
tg --url $client.url checkpoint unwatch sync.get.index.object.filter $object_filter_watch

tg --url $client.url checkpoint continue sync.get.input.object $object_input_watch 0
tg --url $client.url checkpoint unwatch sync.get.input.object $object_input_watch

# The pull can finish only if the file bypasses the blocked local index request.
let object_output = job recv --tag $object_pull --timeout 10sec
success $object_output "the available object must bypass the local index request"
tg --url $client.url checkpoint unwatch sync.get.index.object $object_index_watch

# A process with no object output becomes available as soon as its data enters the graph.
let process = "pcs_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0"
let process_data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	finished_at: 0,
	host: "test",
	output: 5,
	sandbox: "sbx_00041061050r3gg28a1c60t3gf20",
	status: "finished",
}
tg --url $remote.url process put $process ($process_data | to json)
tg --url $remote.url index

let process_index_watch = (
	tg --url $client.url checkpoint watch sync.get.index.process --params ({ id: $process } | to json)
	| from json
	| get watch
)

let process_pull = job spawn {
	let job_id = job id
	let output = tg --url $client.url pull $process | complete
	$output | job send --tag $job_id 0
}

# The pull can finish only if the process bypasses the blocked local index request.
let process_output = job recv --tag $process_pull --timeout 10sec
success $process_output "the available process must bypass the local index request"
tg --url $client.url checkpoint unwatch sync.get.index.process $process_index_watch
