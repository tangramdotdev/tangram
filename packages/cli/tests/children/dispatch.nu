use ../../test.nu *

# The top level children command dispatches a reference to object children or process children.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("dispatch");'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process

# A path reference resolves to object children, a list of ids.
let object_children = tg children $path | from json
assert (($object_children | get 0) | str starts-with "fil_") "the path reference should resolve to object children"

# A process id resolves to process children, a list of referents.
let process_children = tg children $build.process | from json
assert equal $process_children [] "the process id should resolve to process children"
