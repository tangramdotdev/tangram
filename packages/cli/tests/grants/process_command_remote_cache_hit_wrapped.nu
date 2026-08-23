use ../../test.nu *

# A process can pass a new object wrapping its own remote cache-hit child's output to another child's command.

let remote = spawn --name remote

let path = artifact {
	tangram.ts: '
		export function dependency() {
			return tg.directory({ file: tg.file("hello") });
		}

		export async function consume(wrapper: tg.Directory) {
			let directory = await wrapper.get("dependency").then(tg.Directory.expect);
			let file = await directory.get("file").then(tg.File.expect);
			return file.text;
		}

		export default async function () {
			let directory = await tg.build(dependency).then(tg.Directory.expect);
			let wrapper = await tg.directory({ dependency: directory });
			return tg.build(consume, wrapper);
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
assert equal ($children | length) 2 "the build should have spawned the dependency and consumer."
assert (($children | first).cached? | default false) "the dependency should be a remote cache hit."
assert not (($children | last).cached? | default false) "the consumer should run locally."
assert equal $result.exit 0 "a process should be able to pass a wrapper around its cache-hit child's output to another child."
assert equal $result.output "hello" "the consumer should be able to read its command argument."
