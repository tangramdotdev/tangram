use ../../test.nu *

# Process metadata after a build and indexing reports subtrees for the command, error, log, and output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.file("metadata"); }'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process
tg index

let metadata = tg process metadata $build.process | from json
assert equal ($metadata.node | columns) [command error log output] "the node should report all of the component subtrees"
assert equal $metadata.node.output.count 2 "the output subtree should count the file and its blob"
assert equal $metadata.node.error.count 0 "a successful build should have no error objects"
assert equal $metadata.subtree.count 1 "the process subtree should count only the process itself"
