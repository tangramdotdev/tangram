use ../../test.nu *

# Recursively pulling a tagged process brings the whole process tree present locally and records the tag locally.

let remote = spawn --cloud --name remote
let local = spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.build(x);
			return tg.file("root output");
		}
		export async function x() { return tg.file("child output"); }
	',
}
let process = tg --url $remote.url build --detach $path | str trim
tg --url $remote.url wait $process
tg --url $remote.url tag tree/1.0.0 $process
let child = tg --url $remote.url get $process | from json | get children | first | get process

tg pull --recursive tree/1.0.0

# The child process is present locally.
let local_child = tg process get --local $child | complete
success $local_child "the child process should be present locally after a recursive tag pull"

# The tag resolves locally to the pulled process.
let tag = tg tag get tree/1.0.0 | from json
assert ($tag.item.id == $process) "the tag should resolve locally to the pulled process"
