use ../../test.nu *

# A process can return a node inside the output of its own remote cache-hit child.
#
# The child build is a remote cache hit, so its output directory arrives by pull. Finishing the
# parent requires Object(Subtree) on the value it returns, and the file inside that directory has no
# grant of its own. Returning the directory itself succeeds, so only a node beneath it is blocked.

let remote = spawn --name remote

let path = artifact {
	tangram.ts: '
		export function dependency() {
			return tg.directory({ file: tg.file("hello") });
		}

		export default async function () {
			let directory = await tg.build(dependency).then(tg.Directory.expect);
			return await directory.get("file");
		}
	',
}

# Populate the remote cache for the dependency.
let dependency = tg --url $remote.url build --detach $"($path)#dependency" | str trim
tg --url $remote.url wait $dependency | ignore

let cold = spawn --name cold --config {
	remotes: { default: { url: $remote.url } },
}

let result = tg --url $cold.url build $path | complete
success $result "a process should be able to return a node inside its cache-hit child's output."
assert equal (tg --url $cold.url cat ($result.stdout | str trim) | str trim) "hello" "the returned file should hold the child's contents."
