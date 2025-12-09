use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => {};'
}

let output = tg build $path
snapshot $output
