use ../../test.nu *

# A process that spawns no children has an empty children list.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return "leaf"; }'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process

let children = tg process children $build.process | from json
assert equal $children [] "the process should have no children"
