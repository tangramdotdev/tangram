use ../../test.nu *

# When --detach is set, `tg run` should print the process ID and return immediately.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "hello";'
}

let sandbox_output = tg run --detach $path --sandbox | str trim
assert ($sandbox_output | str starts-with "pcs_")

let output = tg run --detach $path | str trim
assert ($output | str starts-with "pcs_")
