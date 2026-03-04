use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "hello, world!";'
}

let id = tg checkin $path
tg index

let sandbox_output = tg run $id --sandbox
snapshot $sandbox_output '"hello, world!"'

let output = tg run $id
assert equal $output $sandbox_output
