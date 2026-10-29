use ../../util/test.nu *

# Building or running a typed command checks its args and output in TypeScript and Python.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export function greet(name: string) { return tg.file(`Hello, ${name}!`); }
		export default async function () {
			const command = (await tg.command(greet)) as tg.Command<[string], tg.File>;
			const built: tg.File = await tg.build(command).arg("World");
			const ran: tg.File = await tg.run(command).arg("World");
			const method: tg.File = await command.build("World");
			return [await built.text, await ran.text, await method.text];
		}
	'
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) ['Hello, World!' 'Hello, World!' 'Hello, World!']

let path = artifact {
	tangram.ts: '
		export function greet(name: string) { return tg.file(`Hello, ${name}!`); }
		export default async function () {
			const command = (await tg.command(greet)) as tg.Command<[string], tg.File>;
			const count: number = await tg.build(command).arg("World");
			await command.build(42);
		}
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:4:') $output.stderr
assert ($output.stderr | str contains 'tangram.ts:5:') $output.stderr

let path = artifact {
	tangram.py: '
		import typing
		async def greet(name: str) -> tg.File:
		    return await tg.file(f"Hello, {name}!")
		async def default():
		    command = typing.cast(tg.Command[tuple[str], tg.File], await tg.command(greet))
		    built: tg.File = await tg.build(command).arg("World")
		    method: tg.File = await command.build("World")
		    return [await built.text(), await method.text()]
	'
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) ['Hello, World!' 'Hello, World!']

let path = artifact {
	tangram.py: '
		import typing
		async def greet(name: str) -> tg.File:
		    return await tg.file(f"Hello, {name}!")
		async def default():
		    command = typing.cast(tg.Command[tuple[str], tg.File], await tg.command(greet))
		    count: int = await command.build("World")
		    await command.build(42)
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.py:6:') $output.stderr
assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr
