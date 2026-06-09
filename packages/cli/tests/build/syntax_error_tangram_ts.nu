use ../../test.nu *

# Building a package whose root module has a syntax error fails with a diagnostic instead of succeeding or hanging.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
	'
}

let output = tg build $path | complete
failure $output
