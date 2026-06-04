use ../../test.nu *

# A build whose default export returns a Uint8Array produces the expected serialized output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => new Uint8Array([1,2,3]);'
}

let output = tg build $path
snapshot $output
