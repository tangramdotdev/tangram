use ../lib/test.nu *

# A remote build that finishes before its command transfer reports the process result instead of the transfer's interruption.
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true },
}
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
	remotes: { default: { url: $remote.url } },
}

let path = artifact {
	tangram.ts: 'export default () => "hello"',
}

# Hold the end of the command transfer and observe when the process finishes.
let transfer_watch = (
	tg --url $local.url checkpoint watch sync.put.end
	| from json
	| get watch
)
let finish_watch = (
	tg --url $remote.url checkpoint watch runner.process.output.stored
	| from json
	| get watch
)

let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

# The process finishes while the end of the command transfer is held.
let output = timeout 60s tg --url $local.url checkpoint wait sync.put.end $transfer_watch 0 | complete
success $output "the command transfer should send its objects"
let output = timeout 60s tg --url $remote.url checkpoint wait runner.process.output.stored $finish_watch 0 | complete
success $output "the process should finish while the end of the command transfer is held"
tg --url $remote.url checkpoint continue runner.process.output.stored $finish_watch 0
tg --url $remote.url checkpoint unwatch runner.process.output.stored $finish_watch
tg --url $local.url checkpoint continue sync.put.end $transfer_watch 0
tg --url $local.url checkpoint unwatch sync.put.end $transfer_watch

let output = try { job recv --tag $build --timeout 60sec } catch { null }
assert ($output != null) "the build should finish"
success $output "the build should succeed"
assert not ($output.stderr | str contains "put end message") "the build should not report the interrupted command transfer"
