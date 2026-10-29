use ../../lib/test.nu *

# A trusted runner re-signs a remote token locally, so a second principal can use a stored artifact without pulling it again.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json

let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf \"%s\" \"$1\" > \"$TANGRAM_OUTPUT\""), "executable": true })'
let artifact_a = tg --url $remote.url --token $alice.token put --no-tokens $artifact | referent node

let output = tg --url $remote.url --token $alice.token build $artifact_a --arg-string alice | complete
success $output "Alice's process should store artifact A on the runner"
tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $artifact_a
tg --url $remote.url index
tg --url $runner.url index
assert equal (tg --url $runner.url availability --local $artifact_a | from json) { subtree: true } "artifact A should be stored on the runner"

let build = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $bob.token build $artifact_a --arg-string bob | complete
	$output | job send --tag $job_id 0
}

let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "Bob's build blocked while re-pulling artifact A" }
}
success $output "Bob's process should use the locally stored artifact A"
