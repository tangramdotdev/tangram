use ../../test.nu *

# Waiting for a process through a tag preserves the resolved location.

let origin = server spawn --name origin
let sink = server spawn --name sink
let local = server spawn --name local
tg --url $local.url remote put default $sink.url
tg --url $local.url remote put origin $origin.url

let path = artifact {
	tangram.ts: 'export default async function () { return 42; }',
}
let process = tg --url $origin.url build --detach $path | str trim
tg --url $origin.url tag put wait_process $process

let output = tg --url $local.url wait 'wait_process?location=remote:origin' | from json
assert equal $output.exit 0 "waiting through the tag should succeed"
assert equal $output.output 42 "waiting through the tag should return the process output"

let process_data = tg --url $local.url process get 'wait_process?location=remote:origin' | from json
assert equal $process_data.output 42 "a process subcommand should preserve the resolved location"

let output = tg --url $local.url wait $'($process)?location=remote:origin' | from json
assert equal $output.exit 0 "waiting through a remote process reference should succeed"
assert equal $output.output 42 "waiting through a remote process reference should return the process output"
