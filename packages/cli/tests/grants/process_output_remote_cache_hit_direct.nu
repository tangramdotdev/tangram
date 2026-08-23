use ../../test.nu *

# A process can return the output of its own remote cache-hit child directly.

let remote = spawn --name remote

let path = artifact {
	tangram.ts: '
		export function dependency() {
			return tg.directory({ file: tg.file("hello") });
		}

		export default async function () {
			return tg.build(dependency);
		}
	',
}

# Populate the remote cache for the dependency.
let dependency = tg --url $remote.url build --detach $"($path)#dependency" | str trim
tg --url $remote.url wait $dependency | ignore

let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

let process = tg --url $local.url build --detach $path | str trim
let result = tg --url $local.url wait $process | from json
let children = tg --url $local.url process children $process | from json
assert equal ($children | length) 1 "the build should have spawned the dependency."
assert ($children | first | get cached) "the dependency should be a remote cache hit."
assert equal $result.exit 0 "a process should be able to return its cache-hit child's output directly."
let output = tg --url $local.url get $result.output.value --depth inf | complete
success $output "the returned child output should be readable."
