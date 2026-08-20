use ../../test.nu *

# A process can return a child of the output of its own remote cache-hit child. Resolving the entry
# pulls the output directory node, but the returned file remains remote until it is read.

let remote = spawn --name remote

let path = artifact {
	tangram.ts: '
		export function dependency() {
			return tg.directory({ file: tg.file("hello") });
		}

		export default async function () {
			let directory = await tg.build(dependency).then(tg.Directory.expect);
			return directory.get("file");
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
assert equal $result.exit 0 "a process should be able to return a child of its cache-hit child's output."
assert equal (tg --url $local.url cat $result.output.value | str trim) "hello" "the returned file should hold the child's contents."
