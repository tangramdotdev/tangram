use ../lib/test.nu *

# Waiting for a process through a tag preserves the resolved location.

let local_origin = server spawn --name local-origin
let remote_sink = server spawn --name remote-sink
let local = server spawn --name local
tg --url $local.url remote put default $remote_sink.url
tg --url $local.url remote put origin $local_origin.url

let path = artifact {
	tangram.ts: 'export default async function () { return 42; }',
}
let process = tg --url $local_origin.url build --no-tokens --detach $path | referent node
tg --url $local_origin.url tag put wait_process $process

let output = tg --url $local.url wait 'wait_process?location=remote:origin' | from json
assert equal $output.exit 0 "waiting through the tag should succeed"
assert equal $output.output 42 "waiting through the tag should return the process output"

let process_data = tg --url $local.url process get 'wait_process?location=remote:origin' | from json
assert equal $process_data.output 42 "a process subcommand should preserve the resolved location"

let output = tg --url $local.url wait $'($process)?location=remote:origin' | from json
assert equal $output.exit 0 "waiting through a remote process reference should succeed"
assert equal $output.output 42 "waiting through a remote process reference should return the process output"
