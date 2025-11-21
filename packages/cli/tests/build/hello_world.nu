use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "Hello, World!"'
}

let output = run tg build $path
snapshot $output
