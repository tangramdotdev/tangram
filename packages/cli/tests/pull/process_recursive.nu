use ../../test.nu *

# Pulling a process recursively brings its child processes present locally.

let remote = server spawn --cloud --name remote
let source = server spawn --name source --config {
	remotes: { default: { url: $remote.url } },
}
let local = server spawn --name local
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
let process = tg --url $source.url build --detach $path | str trim
tg --url $source.url wait $process
tg --url $source.url push --process-children $process
tg --url $remote.url wait $process
let child = tg --url $remote.url get $process | from json | get children | first | get process

tg pull --process-children $process

let local_child = tg process get --local $child | complete
success $local_child "the child process should be present locally after a recursive pull"
