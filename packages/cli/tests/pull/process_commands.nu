use ../../test.nu *

# Pulling a process with the commands flag brings the process command present locally.

let remote = spawn --cloud --name remote
let local = spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: 'export default async () => tg.file("from remote build");',
}
let process = tg --url $remote.url build --detach $path | str trim
tg --url $remote.url wait $process
let command = tg --url $remote.url get $process | from json | get command

tg pull --commands $process

let local_command = tg object get --local $command | complete
success $local_command "the command should be present locally after a pull with commands"
