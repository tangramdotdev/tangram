use ../lib/test.nu *

# A process can use an object from its own remote cache-hit child after receiving a local grant.
#
# Pulling the file stores its bytes but does not grant it to later processes. The explicit public
# grant supplies the local authorization that may flow through the cache-hit child.

let remote = server spawn --busybox --name remote

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

let local_cold = server spawn --busybox --name local-cold --config {
	remotes: { default: { url: $remote.url } }
}
tg --url $local_cold.url pull $output
tg --url $local_cold.url index

# With the object stored, the only way to fail below is the authorization check.
assert equal (tg --url $local_cold.url availability --local $output | from json) { subtree: true } "the pulled object should be available on the cold client."
tg --url $local_cold.url grant public object_subtree $output

let result = tg --url $local_cold.url build $path | complete
success $result "a process should be able to use a locally granted object from its cache-hit child."
assert equal (tg --url $local_cold.url cat ($result.stdout | str trim) | str trim) "hello" "the process should read the pulled file."
