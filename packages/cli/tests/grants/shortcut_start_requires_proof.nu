use ../lib/test.nu *

# A runner must not start a shortcut child of a process that it does not run, since the child would receive the parent's authority over the command's objects, or in a sandbox that it does not host, since the sandbox's owner is billed for the child.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux sandboxes'
}

const helper = path self '../lib/process_control_start.py'
let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let mallory = tg login --verbose --name mallory | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode

# Alice runs a sandboxed process whose command references her private file.
let secret = tg --token $alice.token put --no-tokens 'tg.file("shortcutsecret")' | referent node
let parent = tg --token $alice.token spawn --sandbox --verbose --executable $secret | from json | get process | split row '?' | first
tg --token $alice.token wait $parent | complete
tg index
let data = tg --token $alice.token get --no-tokens $parent | from json
failure (tg --token $mallory.token get $secret | complete) "Mallory should not read Alice's private file."

# Mallory's runner tries to start a shortcut child that names Alice's process as its parent.
let runner = tg --token $mallory.token runner create --owner $mallory.user.id | from json
let lease = $parent | str substring 6..

# The runner must not start the child without the parent's token, or with a token that is not the parent's.
for token in [null $mallory.token] {
	let start = { data: $data, kind: start, lease: $lease, parent: $parent, parent_token: $token } | compact
	{ location: 'local', mode: $start } | to json | save --force arg.json
	let output = python3 $helper $socket $runner.token.token arg.json | complete
	failure $output "a runner should not start a shortcut child of a process that it does not run"
}

# Mallory runs her own process, whose token proves that her runner runs it.
let own = tg --token $mallory.token spawn --sandbox --verbose --executable /bin/sh -- -c 'echo "$TANGRAM_TOKEN"; sleep 60' | from json | get process | split row '?' | first
wait_until { (tg --token $mallory.token log $own | complete).stdout | str trim | is-not-empty } "Mallory's process should print its token"
let own_token = tg --token $mallory.token log $own | str trim

# The runner must not start a child of its own process in Alice's sandbox without the sandbox's token, or with a token that is not the sandbox's.
for token in [null $own_token] {
	let start = { data: $data, kind: start, lease: $lease, parent: $own, parent_token: $own_token, sandbox_token: $token } | compact
	{ location: 'local', mode: $start } | to json | save --force arg.json
	let output = python3 $helper $socket $runner.token.token arg.json | complete
	failure $output "a runner should not start a shortcut child in a sandbox that it does not host"
}
tg --token $mallory.token cancel $own | complete
