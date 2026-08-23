use ../../test.nu *

# The availability commands report local object and process availability and dispatch by reference kind.

let server = spawn

let file = tg put 'tg.file("available")' | str trim
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("available"); }'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process
tg index

let object = tg object availability $file | from json
assert equal $object.subtree true "the object availability command should report that the subtree is available"

let dispatched_object = tg availability $file | from json
assert equal $dispatched_object $object "the top-level availability command should dispatch object ids"

let process = tg process availability $build.process | from json
assert equal $process.subtree true "the process availability command should report that the process subtree is available"
assert equal $process.node_command true "the process availability command should report that the command subtree is available"
assert equal $process.node_output true "the process availability command should report that the output subtree is available"

let dispatched_process = tg availability $build.process | from json
assert equal $dispatched_process $process "the top-level availability command should dispatch process ids"
