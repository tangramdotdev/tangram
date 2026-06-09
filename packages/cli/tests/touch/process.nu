use ../../test.nu *

# Touching an existing process succeeds.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("touched");'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process

let output = tg process touch $build.process | complete
success $output
