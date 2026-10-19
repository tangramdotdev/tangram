use ../lib/test.nu *

let local = server spawn --config {
	compiler: {
		check_backend: tsrs
		typescript_executable: /__missing_typescript7__/tsc
	}
}

let path = artifact {
	value.tg.ts: 'export const value = 42;'
	tangram.ts: '
		import { value } from "./value.tg.ts";
		const number: number = value;
		export default function () { return tg.file(`${number}`); }
	'
}

success (tg check $path | complete)

# Dependency edits must be visible to subsequent checks.
'export const value: number = "wrong";' | save --force ($path | path join value.tg.ts)
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'.")
assert ($output.stderr | str contains 'value.tg.ts')

# Non-BMP characters must preserve diagnostic source locations.
'const face = "😀"; export const value: number = "wrong";' | save --force ($path | path join value.tg.ts)
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'.")
assert ($output.stderr | str contains 'value.tg.ts')

'export const value = 43;' | save --force ($path | path join value.tg.ts)
success (tg check $path | complete)

'import "./missing.tg.ts"; export default 0;' | save --force ($path | path join tangram.ts)
failure (tg check $path | complete)
