use ../../test.nu *

# Process metadata for a process that has not been indexed succeeds with an empty object.

let server = spawn --config { indexer: false }

let path = artifact {
	tangram.ts: 'export default function () { return tg.file("unindexed"); }'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process

let output = tg process metadata $build.process | complete
success $output
assert equal ($output.stdout | from json | columns) [] "the metadata should be empty before indexing"
