use ../../util/test.nu *

# An export that is not a value can be used in its own language, the other exports of its module can be used from another language, and using it from another language fails checking with the reason that it is not a value.

let local = server spawn

# A TypeScript module uses a Python module that exports a function that is not a value.
let path = artifact {
	tangram.ts: '
		import { greet } from "./greet.tg.py";
		export default () => greet.build("world");
	'
	greet.tg.py: '
		from collections.abc import Callable
		__all__ = ["curry", "greet"]
		def curry(name: str) -> Callable[[], str]:
		    return lambda: f"Hello, {name}!"
		def greet(name: str) -> str:
		    return curry(name)()
	'
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, world!'

let path = artifact {
	tangram.ts: '
		import { curry } from "./greet.tg.py";
		export default () => curry.build("world");
	'
	greet.tg.py: '
		from collections.abc import Callable
		__all__ = ["curry"]
		def curry(name: str) -> Callable[[], str]:
		    return lambda: f"Hello, {name}!"
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:2:') $output.stderr
assert ($output.stderr | str contains 'a function is not a value') $output.stderr

# A Python module uses a TypeScript module that exports a function that is not a value.
let path = artifact {
	tangram.py: '
		# /// script
		# [tool.tangram.dependencies.greet]
		# specifier = "./greet.tg.ts"
		# ///
		from greet import greet
		async def default() -> str:
		    return await greet.build("world")
	'
	greet.tg.ts: '
		export function curry(name: string) { return () => `Hello, ${name}!`; }
		export function greet(name: string) { return curry(name)(); }
	'
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, world!'

let path = artifact {
	tangram.py: '
		# /// script
		# [tool.tangram.dependencies.greet]
		# specifier = "./greet.tg.ts"
		# ///
		from greet import curry
		async def default() -> str:
		    return await curry.build("world")
	'
	greet.tg.ts: '
		export function curry(name: string) { return () => `Hello, ${name}!`; }
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr
assert ($output.stderr | str contains 'a function is not a value') $output.stderr
