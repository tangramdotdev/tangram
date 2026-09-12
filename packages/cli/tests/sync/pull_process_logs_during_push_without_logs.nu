use ../../test.nu *

# A push of a process without its logs is held before the process reaches the remote. A pull that asks for the logs does not wait for the push, because the held sync answers that it does not carry the logs. A pull that asks only for what the push carries waits for the process to arrive.

let root_token = random chars

# The remote waits a long time for a held sync so that a prompt answer cannot come from the timeout.
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { item_get_timeout: 120 },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let alice_local = server spawn --name alice-local --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let bob = tg --url $remote.url login --verbose --name bob | from json
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
}

# Alice builds a process that writes a log.
let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("hello");
			return "hello";
		};
	'
}
let process = tg --url $alice_local.url build --detach $path | str trim
tg --url $alice_local.url wait $process | ignore

# Hold the process's store read on alice's server so that the push stays open before the process reaches the remote.
let watch = (
	tg --url $alice_local.url checkpoint watch sync.put.store.process --params ({ id: $process } | to json)
	| from json
	| get watch
)

# Alice pushes the process without its logs and takes the referent with the sync token from the log.
let push_log = $env.TMPDIR | path join push.log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $alice_local.url push $process o+e>| tee { save --force $push_log } | complete
	$output | job send --tag $job_id 0
}
let output = timeout 30s tg --url $alice_local.url checkpoint wait sync.put.store.process $watch 0 | complete
success $output "alice's push should reach the process"
let push_lines = open --raw $push_log | lines | where {|line| $line =~ "sync" }
if ($push_lines | is-empty) {
	error make { msg: $"the push should log the referent with the sync token:\n(open --raw $push_log)" }
}
let referent = $push_lines | first | str trim

# Bob's pull asks for the logs, which the push does not carry, so it completes while the push is held.
let output = timeout 15s tg --url $bob_local.url pull --process-logs $referent | complete
if $output.exit_code == 124 {
	error make { msg: "the pull should not wait for a push that does not carry the logs" }
}
failure $output "the pull should not find the process with its logs"

# Bob's pull for what the push carries waits for the process.
let pull = job spawn {
	let job_id = job id
	let output = tg --url $bob_local.url pull $referent | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $pull --timeout 5sec } catch { null }
if $output != null {
	error make { msg: $"the pull should wait while the push is held: ($output)" }
}

# Release the process. Both the push and the pull complete.
tg --url $alice_local.url checkpoint continue sync.put.store.process $watch 0
tg --url $alice_local.url checkpoint unwatch sync.put.store.process $watch
success (job recv --tag $push --timeout 30sec) "alice's push should complete"
success (job recv --tag $pull --timeout 30sec) "bob's pull should complete"
