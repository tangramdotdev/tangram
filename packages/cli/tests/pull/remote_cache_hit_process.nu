use ../../test.nu *

# Pulling a remote cache-hit process replaces its local index-only record with complete process data.

let remote = spawn --name remote

let path = artifact {
	tangram.ts: '
		export function dependency() { return tg.file("dependency"); }
		export default async function () { return tg.build(dependency); }
	',
}

# Populate the remote cache for the dependency.
let remote_dependency = tg --url $remote.url build --detach $"($path)#dependency" | str trim
tg --url $remote.url wait $remote_dependency | ignore

# Reuse the dependency on a clean client, creating an index-only process record locally.
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}
let parent = tg --url $local.url build --detach $path | str trim
tg --url $local.url wait $parent | ignore
let child = tg --url $local.url process children $parent | from json | first
assert $child.cached "the dependency should be a remote cache hit"
let child_id = $child.process | split row '?' | first
let remote_dependency_id = $remote_dependency | split row '?' | first
assert equal $child_id $remote_dependency_id "the child should reuse the remote process"

tg --url $local.url index

tg --url $local.url pull $child.process

let local_process = tg --url $local.url process get --local $child_id | complete
success $local_process "the pulled cache-hit process should have complete data locally"
assert equal ($local_process.stdout | from json | get status) finished "the local process data should be finished"
