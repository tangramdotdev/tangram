use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "hello, world!";'
}

let sandbox_output = tg run $path --sandbox
snapshot $sandbox_output '"hello, world!"'

let output = tg run $path
assert equal $output $sandbox_output
