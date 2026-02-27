use ../../test.nu *

# When --detach --verbose are set, `tg run` should print the full spawn output as JSON.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "hello";'
}

let sandbox_output = tg run --detach --verbose $path --sandbox | from json
assert ($sandbox_output.process | str starts-with "pcs_")

let output = tg run --detach --verbose $path | from json
assert ($output.process | str starts-with "pcs_")
