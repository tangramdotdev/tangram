use ../../test.nu *

# An explicit `undefined` inside a value is rejected at the boundary rather than coerced to `null`. A map with a `null` entry round-trips (see command_null_map_arg_*), but an `undefined` entry must fail.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			// Set a map entry to `undefined` explicitly, casting to bypass the type checker.
			return { a: undefined } as any;
		};
	'
}

let output = tg build $path | complete
failure $output "an explicit undefined value should be rejected, not coerced to null"
assert ($output.stderr | str contains "invalid value")
