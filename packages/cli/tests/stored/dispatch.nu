use ../../test.nu *

# The stored commands report local object and process availability and dispatch by reference kind.

let server = spawn

let file = tg put 'tg.file("stored")' | str trim
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("stored"); }'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process
tg index

let object = tg object stored $file | from json
assert equal $object.subtree true "the object stored command should report that the subtree is stored"

let dispatched_object = tg stored $file | from json
assert equal $dispatched_object $object "the top-level stored command should dispatch object ids"

let process = tg process stored $build.process | from json
assert equal $process.subtree true "the process stored command should report that the process subtree is stored"
assert equal $process.node_command true "the process stored command should report that the command subtree is stored"
assert equal $process.node_output true "the process stored command should report that the output subtree is stored"

let dispatched_process = tg stored $build.process | from json
assert equal $dispatched_process $process "the top-level stored command should dispatch process ids"
