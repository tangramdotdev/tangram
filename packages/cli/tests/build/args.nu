use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default (name: string) => `Hello, ${name}!`;'
}

let output = tg build $path -a 'Tangram' | complete
success $output
snapshot $output.stdout
