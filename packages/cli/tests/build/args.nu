use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default (name: string) => `Hello, ${name}!`;'
}

let output = run tg build $path -a 'Tangram'
snapshot $output
