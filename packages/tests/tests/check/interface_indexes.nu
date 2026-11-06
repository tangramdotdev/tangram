use ../../util/test.nu *

let local = server spawn
let imports = '
	# /// script
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# ///
	from values import echo
'
let caller = $imports + '
	async def default() -> dict[str, str]:
	    return await echo.build({"name": "value", "0": "zero"})
'

# Equivalent string and number indexes describe the same string-keyed map, including indexes produced by mapped types.
for type in [
	'{ [key: string]: string }'
	'{ [key: string]: string; [key: number]: string }'
	'{ [key: number]: string; [key: string]: string }'
	'Omit<{ [key: string]: string }, "unused">'
	'{ [key: string]: tg.Unresolved<string>; [key: number]: tg.Unresolved<string> }'
] {
	let modules = { values.tg.ts: ('export function echo(value: ' + $type + '): ' + $type + ' { return value; }') }
	let path = artifact ($modules | merge { tangram.py: $caller })
	success (tg check $path | complete)

	# Keys, values, and output annotations retain their types.
	for statement in [
		'await echo.build({0: "zero"})'
		'await echo.build({"0": 42})'
		'value: dict[str, float] = await echo.build({"name": "value"})'
	] {
		let source = (doc $imports) + "\nasync def default():\n    " + $statement + "\n"
		let path = artifact ($modules | merge { tangram.py: $source })
		let output = tg check $path | complete
		failure $output
		assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr
	}
}

# A uniform map cannot preserve narrower numeric indexes, other key types, or required fields.
for type in [
	'{ [key: string]: string | number; [key: number]: number }'
	'{ [key: number]: string }'
	'{ [key: string]: string; [key: symbol]: string }'
	'{ [key: string]: string; required: string }'
] {
	let path = artifact {
		values.tg.ts: ('export function echo(value: ' + $type + '): ' + $type + ' { return value; }')
		tangram.py: $caller
	}
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'the type is not a value') $output.stderr
}
