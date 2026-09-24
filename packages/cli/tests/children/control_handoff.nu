use ../lib/test.nu *

# A children request refreshes the index if its control response becomes unavailable during completion.

let root_token = random chars
let owner = server spawn --name owner --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	indexer: { log_compaction: false },
	roles: [api indexer scheduler],
}
let created = tg --url $owner.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $root_token, url: $owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let finish_watch = tg --url $runner.url checkpoint watch runner.process.finish | from json | get watch
let path = artifact { tangram.ts: 'export default async () => { await tg.build(child); }; export function child() { return "child"; }' }
let process = tg --url $owner.url --token $root_token build --detach $path | str trim
let child = timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 0 | from json | get params.process
tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 0
let parent = timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 1 | from json | get params.process
assert equal $parent $process

# Incomplete indexed children and a missing process use the same fallback when control is unavailable.
for id in [$process pcs_010000000000000000000000000000000000000000000000000000] {
	let params = { process: $id } | to json --raw
	let index_watch = tg --url $owner.url --token $root_token checkpoint watch process.get.index --params $params | from json | get watch
	let control_watch = tg --url $owner.url --token $root_token checkpoint watch process.get.control --params $params | from json | get watch
	let get_job = job spawn {
		let job_id = job id
		let output = tg --url $owner.url --token $root_token process children $id | complete
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $owner.url --token $root_token checkpoint wait process.get.index $index_watch 0 | ignore
	tg --url $owner.url --token $root_token checkpoint unwatch process.get.index $index_watch
	let output = job recv --tag $get_job --timeout 5sec
	failure $output "an unavailable indexed children list must be treated as a missing process"
	if $id == $process {
		let output = timeout 5s tg --url $owner.url --token $root_token process get $id | from json
		assert equal $output.status started "get must still use the indexed process without indexed children"
		let output = timeout 5s tg --url $owner.url --token $root_token process status $id | from json
		assert equal $output [started] "status must still use the indexed process without indexed children"
	}
	tg --url $owner.url --token $root_token checkpoint unwatch process.get.control $control_watch
}

# Hold the initial incomplete index snapshot and the live control responses.
let index_watch = tg --url $owner.url --token $root_token checkpoint watch process.get.index --params ({ process: $process } | to json --raw) | from json | get watch
let control_watch = tg --url $owner.url --token $root_token checkpoint watch process.get.control --params ({ process: $process } | to json --raw) | from json | get watch
let get_job = job spawn {
	let job_id = job id
	let output = tg --url $owner.url --token $root_token process children $process | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $owner.url --token $root_token checkpoint wait process.get.index $index_watch 0 | ignore
timeout 10s tg --url $owner.url --token $root_token checkpoint wait process.get.control $control_watch 0 | ignore

# Complete the process before letting the request decide whether to wait for control.
let submitted_watch = tg --url $owner.url --token $root_token checkpoint watch process.control.finish.submitted --params ({ process: $process } | to json --raw) | from json | get watch
tg --url $runner.url checkpoint unwatch runner.process.finish $finish_watch
timeout 10s tg --url $owner.url --token $root_token checkpoint wait process.control.finish.submitted $submitted_watch 0 | ignore
tg --url $owner.url --token $root_token index
tg --url $owner.url --token $root_token checkpoint unwatch process.control.finish.submitted $submitted_watch
tg --url $owner.url --token $root_token checkpoint unwatch process.get.index $index_watch
let output = job recv --tag $get_job --timeout 10sec
success $output "the request must recover from the unavailable control response using the completed index"
let children = $output.stdout | from json | each { get process | split row '?' | first }
assert equal $children [$child]
tg --url $owner.url --token $root_token checkpoint unwatch process.get.control $control_watch
