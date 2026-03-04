use ../../test.nu *

# When --checkout is set but the process returns null, `tg run` should fail with an error.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => {};'
}

let sandbox_output = tg run --checkout $path --sandbox | complete
failure $sandbox_output
assert ($sandbox_output.stderr | str contains "expected an output")

let output = tg run --checkout $path | complete
failure $output
assert ($output.stderr | str contains "expected an output")
