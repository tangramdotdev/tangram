use ../../test.nu *

# A remote process's children stream uses runner notifications and preserves pagination and locations.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	process: { children_wakeup_interval: 3600.0, status_wakeup_interval: 3600.0 },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.build(a);
			await tg.build(b);
			await tg.build(c);
		}
		export function a() { return "a"; }
		export function b() { return "b"; }
		export function c() { return "c"; }
	',
}
let finish_watch = tg --url $runner.url checkpoint watch runner.process.finish | from json | get watch
let process = tg --url $local.url build --remote --detach $path | str trim
let first = timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 0 | from json | get params.process

# Hold control reads on the remote so children can only arrive from runner state.
let response_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.response.publish --params '{"kind":"get_children"}' | from json | get watch
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let stream_job = job spawn {
	let job_id = job id
	let query = { location: 'remote(hint)', size: 1 } | url build-query
	http get --raw --max-time 60sec --unix-socket $socket $'http://localhost/processes/($process)/children?($query)'
	| lines
	| where { ($in starts-with 'data: ') or $in == 'event: end' }
	| each { $in | job send --tag $job_id 0 }
	| ignore
}
let first_chunk = job recv --tag $stream_job --timeout 10sec | str substring 6.. | from json
let snapshot = timeout 10s tg --url $runner.url process children --remote $process | from json
assert equal ($snapshot | length) 1
assert equal ($snapshot.0.process | split row '?' | first) $first
assert ($snapshot.0.process | str contains 'location=remote') "the child must keep its remote location"

tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 0
let second = timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 1 | from json | get params.process
tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 1
let third = timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 2 | from json | get params.process

let second_chunk = job recv --tag $stream_job --timeout 10sec | str substring 6.. | from json
let third_chunk = job recv --tag $stream_job --timeout 10sec | str substring 6.. | from json
let chunks = [$first_chunk $second_chunk $third_chunk]
assert equal ($chunks | get position) [0 1 2]
let listed = $chunks | get data | flatten | each { get process | split row '?' | first }
assert equal $listed [$first $second $third]
let middle = timeout 10s tg --url $runner.url process children --remote --length 1 --position 1 $process | from json
assert equal ($middle.0.process | split row '?' | first) $second
let tail = timeout 10s tg --url $runner.url process children --remote --position=end.-2 --size 1 $process | from json
assert equal ($tail | each { get process | split row '?' | first }) [$second $third]
let empty = timeout 10s tg --url $runner.url process children --remote --length 0 $process | from json
assert equal $empty []

tg --url $remote.url --token $root_token checkpoint unwatch process.control.response.publish $response_watch
tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 2
let parent = timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 3 | from json | get params.process
assert equal $parent $process
let params = { id: $process } | to json --raw
let remote_finish_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.finish --params $params | from json | get watch
tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 3
tg --url $runner.url checkpoint unwatch runner.process.finish $finish_watch
timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $remote_finish_watch 0 | ignore
assert equal (job recv --tag $stream_job --timeout 10sec) 'event: end' "the attached children stream must end before remote completion is published"
tg --url $remote.url --token $root_token checkpoint unwatch process.control.finish $remote_finish_watch
let output = timeout 30s tg --url $local.url wait $process | complete
success $output
