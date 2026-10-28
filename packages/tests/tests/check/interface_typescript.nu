use ../../util/test.nu *

# A Python module that imports a TypeScript module sees the types of its exports.

let local = server spawn

let path = artifact {
	tangram.py: '
		# /// script
		# [tool.tangram.imports.greeting]
		# specifier = "./greeting.tg.ts"
		# ///
		from greeting import greet, greeting
		async def default() -> str:
		    file: tg.File = await greet.build("World")
		    text: str = await greeting.build()
		    return await file.text() + text
	'
	greeting.tg.ts: '
		export async function greet(name: string, punctuation?: string) {
			return tg.file(`Hello, ${name}${punctuation ?? "!"}`);
		}
		export const greeting = "Hi.";
	'
}
success (tg check $path | complete)

let path = artifact {
	tangram.py: '
		# /// script
		# [tool.tangram.imports.greeting]
		# specifier = "./greeting.tg.ts"
		# ///
		from greeting import greet
		async def default():
		    await greet.build(42)
		    count: int = await greet.build("World")
	'
	greeting.tg.ts: '
		export async function greet(name: string) {
			return tg.file(`Hello, ${name}!`);
		}
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr
assert ($output.stderr | str contains 'tangram.py:8:') $output.stderr
