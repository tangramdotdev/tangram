use ../../util/test.nu *

# An interface refers to a named type that another module declares instead of copying it, including a type that the module only reaches through another module, and importers in the other language check against the declaring module's type.

let local = server spawn

# A TypeScript module returns a type that another TypeScript module declares, through a third module.
let path = artifact {
	tangram.py: '
		# /// script
		# [tool.tangram.dependencies.greet]
		# specifier = "./greet.tg.ts"
		# ///
		from greet import greet
		async def default() -> str:
		    greeting = await greet.build("World")
		    return greeting["text"]
	'
	greet.tg.ts: '
		import { make } from "./make.tg.ts";
		export function greet(name: string) { return make(name); }
	'
	make.tg.ts: '
		import type { Greeting } from "./greeting.tg.ts";
		export function make(name: string): Greeting { return { text: `Hello, ${name}!` }; }
	'
	greeting.tg.ts: '
		export interface Greeting { text: string; }
	'
}
let output = tg interface ($path | path join greet.tg.ts) | complete
success $output
let interface = $output.stdout | from json
assert equal ($interface.imports | columns) [greeting]
assert equal $interface.exports.greet.type.output { kind: reference, import: greeting, name: Greeting }
assert ('definitions' not-in $interface)
let output = tg interface ($path | path join greeting.tg.ts) | complete
success $output
assert equal ($output.stdout | from json | get definitions.Greeting) { kind: record, fields: { text: { type: { kind: string } } } }
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, World!'

# A wrong use of the referenced type fails checking in the other language.
let path = artifact {
	tangram.py: '
		# /// script
		# [tool.tangram.dependencies.greet]
		# specifier = "./greet.tg.ts"
		# ///
		from greet import greet
		async def default() -> int:
		    greeting = await greet.build("World")
		    return greeting["text"]
	'
	greet.tg.ts: '
		import type { Greeting } from "./greeting.tg.ts";
		export function greet(name: string): Greeting { return { text: `Hello, ${name}!` }; }
	'
	greeting.tg.ts: '
		export interface Greeting { text: string; }
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.py:8:') $output.stderr

# A Python module returns a typed dict that another Python module declares.
let path = artifact {
	tangram.ts: '
		import { greet } from "./greet.tg.py";
		export default async () => {
			const greeting = await greet.build("World");
			const text: string = greeting.text;
			return text;
		};
	'
	greet.tg.py: '
		# /// script
		# [tool.tangram.dependencies.greeting]
		# specifier = "./greeting.tg.py"
		# ///
		from greeting import Greeting
		__all__ = ["greet"]
		def greet(name: str) -> Greeting:
		    return {"text": f"Hello, {name}!"}
	'
	greeting.tg.py: '
		from typing import TypedDict
		__all__ = ["Greeting"]
		class Greeting(TypedDict, closed=True):
		    text: str
	'
}
let output = tg interface ($path | path join greet.tg.py) | complete
success $output
let interface = $output.stdout | from json
assert equal ($interface.imports | columns) [greeting]
assert equal $interface.exports.greet.type.output { kind: reference, import: greeting, name: Greeting }
let output = tg interface ($path | path join greeting.tg.py) | complete
success $output
let interface = $output.stdout | from json
assert equal ($interface.exports | columns) []
assert equal $interface.definitions.Greeting { kind: record, fields: { text: { type: { kind: string } } } }
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, World!'

# An open typed dict is not a value, because it may have items that are not values.
let path = artifact {
	tangram.py: '
		from typing import TypedDict
		__all__ = ["greet"]
		class Greeting(TypedDict):
		    text: str
		def greet(name: str) -> Greeting:
		    return {"text": f"Hello, {name}!"}
	'
}
let output = tg interface $path | complete
success $output
let greet = $output.stdout | from json | get exports.greet
assert ($greet.error | str contains 'a typed dict must be closed to be a value') $greet.error
