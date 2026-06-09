use ../../test.nu *

# The top level touch command dispatches a reference to an object touch or a process touch.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("dispatch");'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process

# A path reference resolves to an object touch.
let output = tg touch $path | complete
success $output

# A process id resolves to a process touch.
let output = tg touch $build.process | complete
success $output
