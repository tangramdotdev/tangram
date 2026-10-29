use ../../util/test.nu *

# A children request refreshes the index if its control response becomes unavailable during completion.

let root_token = random chars
let local_owner = server spawn --name local-owner --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $local_owner.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $root_token, url: $local_owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let finish_watch = tg --url $runner.url checkpoint watch runner.process.finish | from json | get watch
let path = artifact { tangram.ts: 'export default async () => { await tg.build(child); }; export function child() { return "child"; }' }
let id = checkin push $local_owner $path --token $root_token
let process = tg --url $local_owner.url --token $root_token build --no-tokens --detach $id | referent node
tg --url $runner.url checkpoint wait --timeout 30s runner.process.finish $finish_watch 0 | ignore
tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 0
tg --url $runner.url checkpoint wait --timeout 30s runner.process.finish $finish_watch 1 | ignore

# Incomplete indexed children and a missing process use the same fallback when control is unavailable.
for id in [$process pcs_010000000000000000000000000000000000000000000000000000] {
	let params = { process: $id } | to json --raw
	let index_watch = tg --url $local_owner.url --token $root_token checkpoint watch process.get.index --params $params | from json | get watch
	let control_watch = tg --url $local_owner.url --token $root_token checkpoint watch process.get.control --params $params | from json | get watch
	let get_job = job spawn {
		let job_id = job id
		let output = tg --url $local_owner.url --token $root_token process children $id | complete
		$output | job send --tag $job_id 0
	}
	tg --url $local_owner.url --token $root_token checkpoint wait --timeout 10s process.get.index $index_watch 0 | ignore
	tg --url $local_owner.url --token $root_token checkpoint unwatch process.get.index $index_watch
	let output = job recv --tag $get_job --timeout 5sec
	failure $output "an unavailable indexed children list must be treated as a missing process"
	if $id == $process {
		let output = timeout 5s tg --url $local_owner.url --token $root_token process get $id | from json
		assert equal $output.status started "get must still use the indexed process without indexed children"
		let output = timeout 5s tg --url $local_owner.url --token $root_token process status $id | from json
		assert equal $output [started] "status must still use the indexed process without indexed children"
	}
	tg --url $local_owner.url --token $root_token checkpoint unwatch process.get.control $control_watch
}

# Hold the initial incomplete index snapshot and the live control responses.
let index_watch = tg --url $local_owner.url --token $root_token checkpoint watch process.get.index --params ({ process: $process } | to json --raw) | from json | get watch
let control_watch = tg --url $local_owner.url --token $root_token checkpoint watch process.get.control --params ({ process: $process } | to json --raw) | from json | get watch
let get_job = job spawn {
	let job_id = job id
	let output = tg --url $local_owner.url --token $root_token process children $process | complete
	$output | job send --tag $job_id 0
}
tg --url $local_owner.url --token $root_token checkpoint wait --timeout 10s process.get.index $index_watch 0 | ignore
tg --url $local_owner.url --token $root_token checkpoint wait --timeout 10s process.get.control $control_watch 0 | ignore

# Complete the process before letting the request decide whether to wait for control.
let submitted_watch = tg --url $local_owner.url --token $root_token checkpoint watch process.control.finish.submitted --params ({ process: $process } | to json --raw) | from json | get watch
tg --url $runner.url checkpoint unwatch runner.process.finish $finish_watch
tg --url $local_owner.url --token $root_token checkpoint wait --timeout 10s process.control.finish.submitted $submitted_watch 0 | ignore
tg --url $local_owner.url --token $root_token index
tg --url $local_owner.url --token $root_token checkpoint unwatch process.control.finish.submitted $submitted_watch
tg --url $local_owner.url --token $root_token checkpoint unwatch process.get.index $index_watch
let output = job recv --tag $get_job --timeout 10sec
success $output "the request must recover from the unavailable control response using the completed index"
let children = $output.stdout | from json | each { get process | split row '?' | first }
assert equal ($children | length) 1
assert equal (tg --url $local_owner.url --token $root_token wait $children.0 | from json | get output) child
tg --url $local_owner.url --token $root_token checkpoint unwatch process.get.control $control_watch
