use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => new Uint8Array([1,2,3]);'
}

let output = run tg build $path
snapshot $output
