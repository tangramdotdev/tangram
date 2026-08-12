use ../../test.nu *

# A client that reuses another user's public process from its remote must be able to use that
# process's output. The remote authorizes the cache hit because the process is public, but finishing
# the local parent that holds its output then fails to authorize that output.

let remote = spawn --name remote --config {
	authentication: { users: { providers: { insecure: true } } },
}
let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let path = artifact {
	tangram.ts: 'export function dep() { return tg.file("dep"); }
export default async function () { return tg.build(dep); }'
}

# Alice makes the dependency's process public on the remote.
tg --url $remote.url --token $alice.token build --public $"($path)#dep" | ignore

# The client accesses the remote as Eve, who can reuse Alice's process only because it is public.
let client = spawn --name client --config {
	remotes: { default: { token: $eve.token, url: $remote.url } },
}

# The premise of the test is that the dependency is reused rather than built.
let process = tg --url $client.url build --detach $path | str trim
tg --url $client.url wait $process | ignore
let children = tg --url $client.url process children $process | from json
assert equal ($children | length) 1 "the build should have spawned the dependency"
assert ($children | first | get cached) "the dependency should have been reused from the remote"

let output = tg --url $client.url build $path | complete
success $output "returning the output of a reused process should be authorized"
