use ../lib/test.nu *
use ../lib/command.nu

# Pulling a process with the command objects flag makes the command objects available locally.

let remote = server spawn --cloud --name remote
let local_source = server spawn --name local-source --config {
	remotes: { default: { url: $remote.url } },
}
let local = server spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: 'export default async function () { return tg.file("from remote build"); }',
}
let process = tg --url $local_source.url build --no-tokens --detach $path | referent node
tg --url $local_source.url wait $process
tg --url $local_source.url push --process-command-objects $process
tg --url $remote.url wait $process
let command = tg --url $remote.url get $process | from json | get command | command module-input $in

tg pull --process-command-objects $process

let local_command = tg object get --local $command | complete
success $local_command "the command should be present locally after a pull with command objects"
