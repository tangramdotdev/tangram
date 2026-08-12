use ../../test.nu *

# A client that reuses a process from its remote must be able to use that process's output. The
# client is allowed to reuse the process, but finishing the parent that holds its output then fails
# to authorize that output, so the two decisions disagree and a remote cache is usable only for a
# build with no parent.
#
# Pulling the output object into the client first does not help, because an object is authorized
# locally by a tag that targets it, by a process that produced it under a sandbox the requester owns,
# or by an explicit grant, and a reused remote process leaves the client with none of those.

let remote = spawn --name remote

let path = artifact {
	tangram.ts: 'export function dep() { return tg.file("dep"); }
export default async function () { return tg.build(dep); }'
}

# The remote holds a finished process for the dependency.
tg --url $remote.url build $"($path)#dep" | ignore

# The client starts with an empty cache and the remote as its only source for that process.
let client = spawn --name client --config {
	remotes: { default: { url: $remote.url } },
}

# The premise of the test is that the dependency is reused rather than built.
let process = tg --url $client.url build --detach $path | str trim
tg --url $client.url wait $process | ignore
let children = tg --url $client.url process children $process | from json
assert equal ($children | length) 1 "the build should have spawned the dependency"
assert ($children | first | get cached) "the dependency should have been reused from the remote"

let output = tg --url $client.url build $path | complete
success $output "returning the output of a reused process should be authorized"
