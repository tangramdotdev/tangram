use ../lib/test.nu *

let executable = $env.TANGRAM_TEST_TYPESCRIPT7_EXECUTABLE? | default ''
if ($executable | is-empty) {
	return
}

let local = server spawn --config {
	compiler: {
		check_backend: typescript7
		typescript_executable: $executable
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

'export const value: number = "wrong";' | save --force ($path | path join value.tg.ts)
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'.")
assert ($output.stderr | str contains 'value.tg.ts')

'const face = "😀"; export const value: number = "wrong";' | save --force ($path | path join value.tg.ts)
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'.")

'import "./missing.tg.ts"; export default 0;' | save --force ($path | path join tangram.ts)
failure (tg check $path | complete)
