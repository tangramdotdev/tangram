use ../../test.nu *

# When --build is set but the build output is not an object (e.g. a string), `tg run` should fail.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "not an object";'
}

let sandbox_output = tg run --build $path --sandbox | complete
failure $sandbox_output
assert ($sandbox_output.stderr | str contains "expected the build to output an object")

let output = tg run --build $path | complete
failure $output
assert ($output.stderr | str contains "expected the build to output an object")
