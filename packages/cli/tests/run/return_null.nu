use ../../test.nu *

# When a process returns null, `tg run` should produce no output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => {};'
}

let sandbox_output = tg run $path --sandbox
snapshot $sandbox_output ''

let output = tg run $path
assert equal $output $sandbox_output
