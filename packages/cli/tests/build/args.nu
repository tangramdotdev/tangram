use ../../test.nu *

# A build forwards a command-line argument to the default export function and produces the expected output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default (name: string) => `Hello, ${name}!`;'
}

let output = tg build $path --arg-string 'Tangram'
snapshot $output
