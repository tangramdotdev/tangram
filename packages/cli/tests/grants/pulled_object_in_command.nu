use ../../test.nu *

# A process can use an object that its own child build pulled from a remote.
#
# The tool build is a remote cache hit, so the file the command reads arrives by pull. Caching it
# requires Object(Subtree) on it, and a pulled object carries no grant, was produced by no process
# here, and is named by no tag. Granting object_subtree on it publicly makes this build succeed, so
# the missing grant is the only thing in the way.

let remote = spawn --busybox --name remote

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export function tool() {
			return tg.file("hello");
		}

		export default async function () {
			let file = await tg.build(tool).then(tg.File.expect);
			return tg.run`cat ${file} > $TANGRAM_OUTPUT`.env(tg.build(busybox));
		}
	',
}

let tool = tg --url $remote.url build --detach $"($path)#tool" | str trim
tg --url $remote.url wait $tool
let output = tg --url $remote.url output $tool | from json | get value | split row '?' | get 0

let cold = spawn --busybox --name cold --config {
	remotes: { default: { url: $remote.url } }
}
tg --url $cold.url pull $output
tg --url $cold.url index

# With the object stored, the only way to fail below is the authorization check.
assert equal (tg --url $cold.url stored --local $output | from json) { subtree: true } "the pulled object should be stored on the cold client."

let result = tg --url $cold.url build $path | complete
success $result "a process should be able to use an object its own child build pulled."
assert equal (tg --url $cold.url cat ($result.stdout | str trim) | str trim) "hello" "the process should read the pulled file."
