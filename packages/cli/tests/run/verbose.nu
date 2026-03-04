use ../../test.nu *

# When --verbose is set, `tg run` should print the full wait output as JSON.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "hello";'
}

let sandbox_output = tg run --verbose $path --sandbox | from json
print $sandbox_output
assert equal $sandbox_output.exit 0

let output = tg run --verbose $path | from json
print $output
assert equal $output.exit 0
