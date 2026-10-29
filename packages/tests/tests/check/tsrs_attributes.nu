use ../../lib/test.nu *

let local = server spawn
let dependency = artifact {
	tangram.ts: 'export default 0;'
	number.tg.ts: 'export const value = 42;'
	string.tg.ts: 'export const value = "hello";'
}
tg tag -p tsrs-attributes/1.0.0 $dependency
rm --recursive $dependency

let source = '
	import { value as number } from "tsrs-attributes/^1" with { get: "number.tg.ts" };
	import { value as text } from "tsrs-attributes/^1" with { get: "string.tg.ts" };
	import directory from "tsrs-attributes/^1" with { type: "directory" };
	export { value as exported } from "tsrs-attributes/^1" with { get: "number.tg.ts" };
	const numberValue: number = number;
	const textValue: string = text;
	const directoryValue: tg.Directory = directory;
	const dynamic = await import("tsrs-attributes/^1", { with: { get: "string.tg.ts" } });
	const dynamicValue: string = dynamic.value;
'
let path = artifact {tangram.ts: $source}

# Identical specifiers with distinct attributes must resolve to distinct module types.
success (tg check $path | complete)

let changed = $source | str replace 'const textValue: string' 'const textValue: number'
$changed | save --force ($path | path join tangram.ts)
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'.")

# Attribute edits must change resolution even when the specifier is unchanged.
let changed = $source | str replace 'get: "number.tg.ts"' 'get: "string.tg.ts"'
$changed | save --force ($path | path join tangram.ts)
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'.")

$source | save --force ($path | path join tangram.ts)
success (tg check $path | complete)
