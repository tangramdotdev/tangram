use ../lib/test.nu *

# Cancelling a nested build must not delay shutdown when the child's runner state expires before its final index write.
let server = server spawn --config {
	advanced: { checkpoints: true, single_process: true },
	runner: { process_state_ttl: 0, sandbox_state_ttl: 0 },
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.build(child).sandbox();
		}
		export async function child() {
			console.log("ready");
			await tg.sleep(300);
		}
	',
}

# Attach the cancellation guard and start both processes.
let wait_watch = tg checkpoint watch process.wait.attach | from json | get watch
let start_watch = tg checkpoint watch runner.process.start | from json | get watch
let build = job spawn {
	let job_id = job id
	let output = tg build $path | complete
	$output | job send --tag $job_id 0
}
let parent = timeout 10s tg checkpoint wait process.wait.attach $wait_watch 0 | from json | get params.process
tg checkpoint unwatch process.wait.attach $wait_watch
timeout 10s tg checkpoint wait runner.process.start $start_watch 0 | ignore
tg checkpoint continue runner.process.start $start_watch 0
let child = timeout 10s tg checkpoint wait runner.process.start $start_watch 1 | from json | get params.process
tg checkpoint unwatch runner.process.start $start_watch
wait_until { (tg log $child | complete).stdout | str contains 'ready' } 'the child must start'
let sandbox = tg process get $child | from json | get sandbox
tg index | ignore

# Hold final index writes and the parent's cleanup while the child retires.
let index_watch = tg checkpoint watch index.batch --params '{"finished_process":true}' | from json | get watch
let release_watch = tg checkpoint watch runner.process.child_lease.release --params ({ parent: $parent } | to json --raw) | from json | get watch
let retired_watch = tg checkpoint watch runner.sandbox.processes.finished --params ({ sandbox: $sandbox } | to json --raw) | from json | get watch
let cancel_watch = tg checkpoint watch process.cancel.index --params ({ finished: false, process: $child } | to json --raw) | from json | get watch
let pid = job list | where id == $build | get 0.pids.0
kill --signal 2 $pid
let output = job recv --tag $build --timeout 10sec
assert equal $output.exit_code 130 'the build must exit after Ctrl-C'
timeout 10s tg checkpoint wait runner.process.child_lease.release $release_watch 0 | ignore
timeout 10s tg checkpoint wait runner.sandbox.processes.finished $retired_watch 0 | ignore
tg checkpoint unwatch runner.sandbox.processes.finished $retired_watch

# Let cleanup read the stale index entry after the child's control handler has retired.
tg checkpoint unwatch runner.process.child_lease.release $release_watch
timeout 10s tg checkpoint wait process.cancel.index $cancel_watch 0 | ignore

# Persist completion before shutdown so no checkpoint or index write can hold it open.
tg checkpoint unwatch index.batch $index_watch
tg index | ignore
assert equal (tg process get $child | from json | get status) finished
tg checkpoint unwatch process.cancel.index $cancel_watch
let pid = open --raw ($server.directory | path join lock) | str trim | into int
kill --signal 2 $pid
wait_until { (open --raw $server.exit | str trim) != '' } 'the server must shut down after cancelling the nested build' --timeout 10sec
assert equal (open --raw $server.exit | str trim | into int) 0 'the server must exit successfully'
