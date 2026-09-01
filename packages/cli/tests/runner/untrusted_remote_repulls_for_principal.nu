use ../../test.nu *

# An untrusted runner re-pulls a stored artifact for a second principal to prove that principal's permissions.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json

let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf \"%s\" \"$1\" > \"$TANGRAM_OUTPUT\""), "executable": true })'
let artifact_a = tg --url $remote.url --token $alice.token put $artifact | str trim

let output = tg --url $remote.url --token $alice.token build $artifact_a --arg-string alice | complete
success $output "Alice's process should store artifact A on the runner"
tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $artifact_a
tg --url $remote.url index
tg --url $runner.url index
assert equal (tg --url $runner.url availability --local $artifact_a | from json) { subtree: true } "artifact A should be stored on the runner"

let watch = (
	tg --url $runner.url checkpoint watch sync.get.input.object --params ({ id: $artifact_a } | to json)
	| from json
	| get watch
)
let build = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $bob.token build $artifact_a --arg-string bob | complete
	$output | job send --tag $job_id 0
}

let output = timeout 30s tg --url $runner.url checkpoint wait sync.get.input.object $watch 0 | complete
success $output "Bob's process should re-pull artifact A from an untrusted remote"
tg --url $runner.url checkpoint continue sync.get.input.object $watch 0
tg --url $runner.url checkpoint unwatch sync.get.input.object $watch

let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "Bob's build did not complete after artifact A was transferred" }
}
success $output "Bob's process should complete after separately proving its permissions"
