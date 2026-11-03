use ../../util/test.nu *

# Compatible intersections preserve the command signature and other atomic value types.
let local = server spawn
let modules = {
	values.tg.ts: '
		type Command = tg.Command<[string], string> & { readonly __arg?: { run(): void } };
		type File = { readonly __arg?: string } & tg.File;
		export function command(value: Command): Command { return value; }
		export function file(value: File): File { return value; }
		export function greet(value: string): string { return value; }
		export function wrongArgs(value: number): string { return String(value); }
		export function wrongOutput(value: string): number { return value.length; }
	'
}
let imports = '
	# /// script
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# ///
	from values import command, file, greet, wrongArgs, wrongOutput
'
let caller = (doc $imports) + "\n" + (doc '
	async def default() -> list[str]:
	    result = await command.build(greet)
	    artifact = await file.build(await tg.file("file"))
	    return [await result.build("command"), await artifact.text()]
')
let path = artifact ($modules | merge { tangram.py: $caller })
success (tg check ($path | path join values.tg.ts) | complete)
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) [command file]

# Both the accepted arguments and the returned values retain their types.
for statement in [
	'await command.build(wrongArgs)'
	'await command.build(wrongOutput)'
	'await (await command.build(greet)).build(42)'
	'value: int = await (await command.build(greet)).build("hello")'
] {
	let source = (doc $imports) + "\nasync def default():\n    " + $statement + "\n"
	let path = artifact ($modules | merge { tangram.py: $source })
	let output = tg check $path | complete
	failure $output $statement
	assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr
}

# Required fields, narrowed properties, and callable intersections cannot be erased.
for constraint in ['{ readonly brand: string }' '{ readonly id?: "cmd_invalid" }' '(() => string)'] {
	let source = 'type Command = tg.Command<[string], string> & ' + $constraint + ";\n" + (doc '
		export function input(value: Command): void {}
		export function output(): Command { throw new Error("unused"); }
	')
	let path = artifact { tangram.ts: $source }
	success (tg check $path | complete)
	let output = tg interface --language json $path | complete
	success $output
	let exports = $output.stdout | from json | get exports
	for name in [input output] {
		assert ('error' in ($exports | get $name)) ($exports | to json)
	}
}
