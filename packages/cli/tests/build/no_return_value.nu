use ../../test.nu *

# A build whose default export returns nothing produces the expected null output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => {};'
}

let output = tg build $path
snapshot $output
