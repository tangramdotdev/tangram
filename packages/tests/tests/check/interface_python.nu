use ../../util/test.nu *

# A TypeScript module that imports a Python module sees the types of its exports.

let local = server spawn

let path = artifact {
	tangram.ts: '
		import { greet, greeting } from "./greeting.tg.py";
		export default async function () {
			const file: tg.File = await greet.build("World");
			const text: string = await greeting.build();
			return (await file.text) + text;
		}
	'
	greeting.tg.py: '
		async def greet(name: str, punctuation: str = "!") -> tg.File:
		    return await tg.file(f"Hello, {name}{punctuation}")
		greeting = "Hi."
	'
}
success (tg check $path | complete)

let path = artifact {
	tangram.ts: '
		import { greet } from "./greeting.tg.py";
		export default async function () {
			await greet.build(42);
			const count: number = await greet.build("World");
		}
	'
	greeting.tg.py: '
		async def greet(name: str) -> tg.File:
		    return await tg.file(f"Hello, {name}!")
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:3:') $output.stderr
assert ($output.stderr | str contains 'tangram.ts:4:') $output.stderr
