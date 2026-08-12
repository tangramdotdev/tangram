use ../../test.nu *

# Verify whether a remote runner can spawn a process created by a credentialed user, push its output, and the user can see the result.

let root_token = random chars

# Spawn the remote.
let remote = spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [cleaner http indexer scheduler],
}

# Create the runner and its token.
let created = tg --url $remote.url --token $root_token runner create | from json
let runner_id = $created.runner.id
let runner_token = $created.token.token

# Spawn the runner.
let runner = spawn --name runner --config {
	remotes: { default: { token: $runner_token, url: $remote.url } },
	runner: { id: $runner_id, remote: "default", token: $runner_token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Run a build that returns an object.
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("hello"); }'
}
let result = tg --url $local.url build --remote $path | complete
success $result

# Verify the user can read the output.
let file = $result.stdout | str trim
let output = tg --url $local.url get $file | complete
success $output
snapshot $output.stdout '
	tg.file({"contents":blb_01t10ptmtyxpb108ztd4np15vt0jm9qnfkfny07vr8yp7tebj04dgg})

'
