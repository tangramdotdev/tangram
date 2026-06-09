use ../../test.nu *

# Documenting a process reference fails because a module must be an object.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "ok";
	'
}
let p = tg build --detach --verbose $path | from json
tg wait $p.process

let output = tg document $p.process | complete
failure $output
assert ($output.stderr | str contains "expected an object") "the error should mention the expected object"
