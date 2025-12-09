use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => 42;'
}

# Run tree command.
let output = tg tree $path
snapshot $output
