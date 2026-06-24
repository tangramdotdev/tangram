use ../../test.nu *

# The top level metadata command dispatches a reference to object metadata or process metadata.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.file("dispatch"); }'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process
tg index

# A path reference resolves to object metadata.
let object = tg metadata $path | from json
assert ($object.node.size > 0) "the path reference should resolve to object metadata"

# A process id resolves to process metadata.
let process = tg metadata $build.process | from json
assert equal ($process.node | columns) [command error log output] "the process id should resolve to process metadata"
