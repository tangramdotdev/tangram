use ../../util/test.nu *

# Python checks string literals in imported TypeScript records and command signatures.

let local = server spawn
let escaped = "single' double\" slash\\ newline\n tab\t unicode λ"
let cases = [
	{ type: '"world"', values: [world] }
	{ type: '"world" | "moon"', values: [world moon] }
	{ type: ($escaped | to json -r), values: [$escaped] }
]
let caller = '
	# /// script
	# [tool.tangram.dependencies.echo]
	# specifier = "./echo.tg.ts"
	# ///
	from echo import echo, literal
	async def record() -> str:
	    result = await echo.build({"name": VALUE})
	    return result["name"]
	async def scalar() -> str:
	    return await literal.build(VALUE)
	async def default() -> list[str]:
	    return [await record(), await scalar()]
'

for case in $cases {
	let modules = {
		args.tg.ts: ('export type Arg = { name: TYPE };' | str replace TYPE $case.type)
		echo.tg.ts: ('
			import type { Arg } from "./args.tg.ts";
			export function echo(arg: Arg): Arg { return arg; }
			export function literal(value: TYPE): TYPE { return value; }
		' | str replace -a TYPE $case.type)
	}

	# Every allowed literal passes checking and survives a build, including strings that need escaping.
	for value in $case.values {
		let source = $caller | str replace -a VALUE ($value | to json -r)
		let path = artifact ($modules | merge { tangram.py: $source })
		success (tg check $path | complete)
		let output = tg build $path | complete
		success $output
		assert equal ($output.stdout | from json) [$value $value]
	}

	# Both the record field and the scalar parameter reject a literal outside the allowed set.
	let source = $caller | str replace -a VALUE '"mars"'
	let path = artifact ($modules | merge { tangram.py: $source })
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr
	assert ($output.stderr | str contains 'tangram.py:10:') $output.stderr

	# The returned literals retain their types instead of becoming unchecked values.
	let source = $caller
		| str replace -a VALUE ($case.values.0 | to json -r)
		| str replace -a '-> str:' '-> int:'
		| str replace 'list[str]' 'list[int]'
	let path = artifact ($modules | merge { tangram.py: $source })
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:8:') $output.stderr
	assert ($output.stderr | str contains 'tangram.py:10:') $output.stderr
}
