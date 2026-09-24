use ../lib/test.nu *

# A pull for missing logs fails once the sync completes; a pull for what it carries succeeds.

let root_token = random chars

let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { control: { request_timeout: 120 } },
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

# Grant Bob access independently of the incoming sync.
tg --url $remote.url --token $root_token grant $bob.user.id process_subtree,process_subtree_log $process | ignore

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
wait_until { ($push_log | path exists) and ((open --raw $push_log) | str contains 'tokens[remote][0]') } 'the push should log the referent with the sync token'
let push_lines = open --raw $push_log | lines | where {|line| $line =~ 'tokens\[' }
let referent = $push_lines | first | str trim

# Bob's pull asks for logs that cannot be supplied by this push.
let logs_pull = job spawn {
	let job_id = job id
	let output = tg --url $bob_local.url pull --logs $referent | complete
	$output | job send --tag $job_id 0
}

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

failure (job recv --tag $logs_pull --timeout 30sec) "the completed sync should not prove the missing logs"
