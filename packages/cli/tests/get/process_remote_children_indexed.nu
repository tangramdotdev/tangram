use ../../test.nu *

# Getting a finished process from a remote indexes its complete child list locally.

let remote = server spawn --name remote
let sink = server spawn --name sink
let local = server spawn --name local
tg remote put default $remote.url
tg --url $remote.url remote put default $sink.url

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.build(child);
			return "parent";
		}
		export function child() { return "child"; }
	',
}
let process = tg --url $remote.url build --detach $path | str trim
tg --url $remote.url wait $process
tg --url $remote.url push --process-logs $process
let remote_children = tg --url $remote.url process children --local $process | from json
assert equal ($remote_children | length) 1 "the remote process should have a child"

failure (tg process get --local $process | complete) "the process should initially be absent locally"

tg get $process | ignore
wait_until {
	(tg process get --local $process | complete).exit_code == 0
} --timeout 30sec "getting the remote process should index it locally"

let local_children = tg process children --local $process | from json
assert equal $local_children $remote_children "the indexed process should retain its remote children"
