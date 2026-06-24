use ../../test.nu *

# Pulling a process without flags makes the process record present locally but leaves its command absent.

let remote = spawn --cloud --name remote
let local = spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: 'export default async function () { return tg.file("from remote build"); }',
}
let process = tg --url $remote.url build --detach $path | str trim
tg --url $remote.url wait $process
let command = tg --url $remote.url get $process | from json | get command

tg pull $process

# The process record is present locally.
let local_process = tg process get --local $process | complete
success $local_process "the process should be present locally after the pull"

# A shallow process pull does not bring the command.
let local_command = tg object get --local $command | complete
failure $local_command "a shallow process pull should leave the command absent"
