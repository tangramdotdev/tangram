use ../../util/test.nu *

# A process that starts after its parent finished does not receive the parent's authority over the command's objects.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux sandboxes'
}

const helper = path self '../../util/process_control_start.py'
let root_token = random chars
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { service: { request_timeout: 5 } },
}
let alice = tg login --verbose --name alice | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode

# Alice runs a sandboxed process whose command references her private file, and it finishes without children.
let secret = tg --token $alice.token put --no-tokens 'tg.file("finishedparentsecret")' | referent node
let parent = tg --token $alice.token spawn --sandbox --verbose --executable $secret | from json | get process | split row '?' | first
tg --token $alice.token wait $parent | complete | ignore
tg index
let finished = tg --token $alice.token get --no-tokens $parent | from json
assert equal $finished.status 'finished'

# Start a shortcut child of the finished parent as the root, which is not asked to prove the parent.
let data = $finished | reject --optional children error exit finished_at log output | upsert status started
let shortcut = { parent: { id: $parent, token: '' }, sandbox: { token: '' } }
let start = { data: $data, kind: start, lease: ($parent | str substring 6..), shortcut: $shortcut }
{ location: 'local', mode: $start } | to json | save --force arg.json
let output = python3 $helper $socket $root_token arg.json | complete
success $output
let child_token = $output.stdout | from json | get token
tg index

# Verify that the child cannot read the private file that its finished parent could read, once its unserved command sync cannot prove the file.
failure (tg --token $child_token get $secret | complete) "the child of a finished parent should not read the parent's objects"
