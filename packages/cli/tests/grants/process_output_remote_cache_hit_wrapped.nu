use ../../test.nu *

# A process can return a new object that wraps the output of its own remote cache-hit child.

let remote = server spawn --name remote

let path = artifact {
	tangram.ts: '
		export function dependency() {
			return tg.directory({ file: tg.file("hello") });
		}

		export default async function () {
			let directory = await tg.build(dependency).then(tg.Directory.expect);
			return tg.directory({ dependency: directory });
		}
	',
}

# Populate the remote cache for the dependency.
let dependency = tg --url $remote.url build --detach $"($path)#dependency" | str trim
tg --url $remote.url wait $dependency | ignore

let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

let process = tg --url $local.url build --detach $path | str trim
let result = tg --url $local.url wait $process | from json
let children = tg --url $local.url process children $process | from json
assert equal ($children | length) 1 "the build should have spawned the dependency."
assert ($children | first | get cached) "the dependency should be a remote cache hit."
assert equal $result.exit 0 "a process should be able to wrap its cache-hit child's output in a new object."
let output = tg --url $local.url get $result.output.value --depth inf | complete
success $output "the wrapped child output should be readable."
