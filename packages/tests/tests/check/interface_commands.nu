use ../../util/test.nu *

# A command can be passed to a command in another language, and checking compares the command's types.

let local = server spawn

let path = artifact {
	tangram.ts: '
		import { twice } from "./twice.tg.py";
		export function shout(text: string) { return tg.file(`${text}!`); }
		export default async function () {
			const command = (await tg.command(shout)) as tg.Command<[string], tg.File>;
			const file: tg.File = await twice.build(command, "hey");
			return file.text;
		}
	'
	twice.tg.py: '
		async def twice(command: tg.Command[tuple[str], tg.File], text: str) -> tg.File:
		    first = await command.build(text)
		    return await command.build(await first.text())
	'
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'hey!!'

let path = artifact {
	tangram.ts: '
		import { twice } from "./twice.tg.py";
		export function count(text: string) { return text.length; }
		export default async function () {
			const command = (await tg.command(count)) as tg.Command<[string], number>;
			await twice.build(command, "hey");
		}
	'
	twice.tg.py: '
		async def twice(command: tg.Command[tuple[str], tg.File], text: str) -> tg.File:
		    return await command.build(text)
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:5:') $output.stderr

# Command arguments are contravariant and outputs are covariant.
let path = artifact {
	tangram.py: '
		def default(command: tg.Command[tuple[str | float], str]) -> tg.Command[tuple[str], str | float]:
		    return command
	'
}
success (tg check $path | complete)
for types in [
	'tg.Command[tuple[str | float], str | float]'
	'tg.Command[tuple[str], str]'
] {
	let source = "def default(command: tg.Command[tuple[str], str | float]) -> " + $types + ":\n    return command\n"
	let path = artifact { tangram.py: $source }
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:2:') $output.stderr
}

# All invocation methods use the receiver's record type to check dictionary literals.
for method in [build run spawn exec] {
	for value in ['"value"' '42'] {
		let source = "from typing import TypedDict\nclass Arg(TypedDict, closed=True):\n    text: str\ndef default(command: tg.Command[tuple[Arg], str]):\n    command." + $method + "({\"text\": " + $value + "})\n"
		let path = artifact { tangram.py: $source }
		let output = tg check $path | complete
		if $value == '"value"' {
			success $output
		} else {
			failure $output
			assert ($output.stderr | str contains 'tangram.py:5:') $output.stderr
		}
	}
}

# All invocation methods return typed builders.
for method in [build run spawn exec] {
	let source = "def default(command: tg.Command[tuple[str], str]):\n    command." + $method + "(\"value\").cwd(42)\n"
	let path = artifact { tangram.py: $source }
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:2:') $output.stderr
	assert ($output.stderr | str contains 'Builder.cwd') $output.stderr
}

# All invocation methods can be called through the class with the command as the first argument, and return typed builders.
for method in [build run spawn exec] {
	let source = "def default(command: tg.Command[tuple[str], str]):\n    tg.Command." + $method + "(command, \"value\")\n"
	success (tg check (artifact { tangram.py: $source }) | complete) $method
	let source = "def default(command: tg.Command[tuple[str], str]):\n    tg.Command." + $method + "(command, \"value\").cwd(42)\n"
	let output = tg check (artifact { tangram.py: $source }) | complete
	failure $output $method
	assert ($output.stderr | str contains 'Builder.cwd') $output.stderr
}

# A command called through the class builds.
let path = artifact {
	tangram.ts: '
		import { twice } from "./twice.tg.py";
		export function shout(text: string) { return tg.file(`${text}!`); }
		export default async function () {
			const command = (await tg.command(shout)) as tg.Command<[string], tg.File>;
			const file: tg.File = await twice.build(command, "hey");
			return file.text;
		}
	'
	twice.tg.py: '
		async def twice(command: tg.Command[tuple[str], tg.File], text: str) -> tg.File:
		    return await tg.Command.build(command, text)
	'
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'hey!'
