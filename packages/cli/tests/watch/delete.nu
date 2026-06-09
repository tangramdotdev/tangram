use ../../test.nu *

# Deleting a watch removes it from the watch list.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => 42;'
}
tg checkin $path --watch

let watches = tg watch list | from json
assert equal ($watches | get path) [$path] "the watch should be listed with its path"

let output = tg watch delete $path | complete
success $output

let watches = tg watch list | from json
assert ($watches | is-empty) "the watch list should be empty after the delete"
