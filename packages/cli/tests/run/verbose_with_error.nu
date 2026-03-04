use ../../test.nu *

# When --verbose is set and the process fails, `tg run` should print the full wait output as JSON including the error.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			throw new Error("verbose error");
		};
	'
}

let sandbox_output = tg run --verbose $path --sandbox | complete
success $sandbox_output
let sandbox_json = $sandbox_output.stdout | from json
assert ($sandbox_json.error != null)
assert ($sandbox_json.exit == 1)

let output = tg run --verbose $path | complete
success $output
let json = $output.stdout | from json
assert ($json.error != null)
assert ($json.exit == 1)
