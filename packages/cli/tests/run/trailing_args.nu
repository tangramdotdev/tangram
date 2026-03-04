use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default (name: string) => `Hello, ${name}!`;'
}

let sandbox_output = tg run $path --sandbox -- Tangram
snapshot $sandbox_output '"Hello, Tangram!"'

let output = tg run $path -- Tangram
assert equal $output $sandbox_output
