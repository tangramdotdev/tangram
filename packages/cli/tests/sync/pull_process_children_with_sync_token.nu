use ../lib/test.nu *

# A process pull reads the child list using permissions proven by sync control before the incoming sync finishes.
let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { control: { request_timeout: 120 } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
}

# Only the parent node is transferred, including its child IDs and cached flags.
let process = 'pcs_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0'
let child = 'pcs_00081061050r3gg28a1c60t3gf20'
let children = [{ cached: true, process: $child }]
let data = {
	children: $children,
	command: 'cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0',
	created_at: 0,
	finished_at: 0,
	host: 'test',
	sandbox: 'sbx_00041061050r3gg28a1c60t3gf20',
	status: finished,
}
tg --url $alice_local.url process put $process ($data | to json)
let blocker = tg --url $alice_local.url put 'tg.blob("later")' | str trim

# Keep the push open after the process is stored, before its grants can be indexed.
let stored_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.process --params ({ id: $process } | to json --raw) | from json | get watch
let blocker_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blocker } | to json --raw) | from json | get watch
let push_log = $env.TMPDIR | path join push.log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $alice_local.url push --no-process-errors --no-process-outputs $process $blocker o+e>| tee { save --force $push_log } | complete
	$output | job send --tag $job_id 0
}
success (timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.process $stored_watch 0 | complete) 'the process should be stored'
success (timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $blocker_watch 0 | complete) 'the unrelated object should keep the push open'
wait_until { (open --raw $push_log) =~ 'tokens\[remote\]\[sync\][^\r\n]*\r?\n' } 'the push should log its complete sync token'
let referent = open --raw $push_log | lines | where {|line| $line =~ 'tokens\[remote\]\[sync\]' } | first | str trim

# Bob cannot read the stored process's children using his ordinary authorization.
let output = timeout 10s tg --url $remote.url --token $bob.token process children --local $process | complete
assert ($output.exit_code != 124) 'the unauthorized read should finish'
failure $output 'Bob should lack indexed authorization for the process'

# Bob pulls using only the sync token before the incoming sync finishes.
let pull = job spawn {
	let job_id = job id
	let output = tg --url $bob_local.url pull --no-process-errors --no-process-outputs $referent | complete
	$output | job send --tag $job_id 0
}
let pull_output = job recv --tag $pull --timeout 10sec
success $pull_output 'the pull should complete while the incoming sync is still open'
let output = tg --url $bob_local.url process children --local $process | from json
assert equal ($output | length) 1 'the pull should preserve the child list'
assert equal ($output.0.process | split row '?' | first) $child 'the pull should preserve the child ID'
assert equal $output.0.cached true 'the pull should preserve the cached flag'

# Finish the incoming sync only after the pull has succeeded.
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.process $stored_watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $blocker_watch
success (job recv --tag $push --timeout 10sec) 'the push should finish'
