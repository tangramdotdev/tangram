use ../../test.nu *

# Pulling a process recursively brings its child processes present locally.

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
let child = tg --url $remote.url get $process | from json | get children | first | get process

tg pull --recursive $process

let local_child = tg process get --local $child | complete
success $local_child "the child process should be present locally after a recursive pull"
