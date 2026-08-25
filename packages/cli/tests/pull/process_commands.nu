use ../../test.nu *

# Pulling a process with the commands flag brings the process command present locally.

let remote = server spawn --cloud --name remote
let source = server spawn --name source --config {
	remotes: { default: { url: $remote.url } },
}
let local = server spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: 'export default async function () { return tg.file("from remote build"); }',
}
let process = tg --url $source.url build --detach $path | str trim
tg --url $source.url wait $process
tg --url $source.url push --process-commands $process
tg --url $remote.url wait $process
let command = tg --url $remote.url get $process | from json | get command

tg pull --process-commands $process

let local_command = tg object get --local $command | complete
success $local_command "the command should be present locally after a pull with commands"
