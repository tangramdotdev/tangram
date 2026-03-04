use ../../test.nu *

# When --checkout is set but the process output is not an artifact (e.g. a string), `tg run` should fail.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "just a string";'
}

let sandbox_output = tg run --checkout $path --sandbox | complete
failure $sandbox_output
assert ($sandbox_output.stderr | str contains "expected an artifact")

let output = tg run --checkout $path | complete
failure $output
assert ($output.stderr | str contains "expected an artifact")
