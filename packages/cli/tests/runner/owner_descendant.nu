use ../../test.nu *

# A runner can run sandboxes owned by descendants of its owner but not unrelated principals.

let remote = server spawn --cloud --name remote --config {
	advanced: { single_process: false },
	authentication: { users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}

let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
tg --url $remote.url --token $alice.token organization create tangram
tg --url $remote.url --token $alice.token group create tangram/engineering
let created = tg --url $remote.url --token $alice.token runner create --owner tangram | from json

let organization_runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.runner.id, remote: "default", token: $created.token.token },
}

let alice_local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let path = artifact { tangram.ts: 'export default () => tg.file("hello")' }
let output = tg --url $alice_local.url build --remote --owner tangram/engineering $path | complete
success $output "the organization runner should run a sandbox owned by a descendant group"

def build_background [url: string, owner: string, path: path] {
	job spawn {
		let job_id = job id
		let output = tg --url $url build --remote --owner $owner $path | complete
		$output | job send --tag $job_id 0
	}
}

let bob_local = server spawn --name bob-local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
}
let unrelated_path = artifact { tangram.ts: 'export default () => tg.file("unrelated")' }
let start_watch = (
	tg --url $organization_runner.url checkpoint watch runner.process.start
	| from json
	| get watch
)
let build = build_background $bob_local.url $bob.user.id $unrelated_path
let output = timeout 1s tg --url $organization_runner.url checkpoint wait runner.process.start $start_watch 0 | complete
failure $output "the organization runner should not start an unrelated user's process"

let created = tg --url $remote.url --token $bob.token runner create --owner $bob.user.id | from json
let user_runner = server spawn --name bob_runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.runner.id, remote: "default", token: $created.token.token },
}
let output = try { job recv --tag $build --timeout 10sec } catch { null }
if $output == null {
	error make { msg: "the build did not complete after an eligible runner connected" }
}
success $output "the user runner should run its owner's sandbox"
