use ../../test.nu *

# A build of a default export returning a string produces the expected output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return "Hello, World!"; }'
}

let output = tg build $path
snapshot $output
