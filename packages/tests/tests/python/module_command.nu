use ../../util/test.nu *

# A command builder for an export of a module runs the export with its args.

let local = server spawn

let path = artifact {
	tangram.py: '
		async def default() -> str:
		    command = tg.Command.Builder.with_module(__tangram_module__, "greet", ["World", 1])
		    return await command.build()

		def greet(name: str, count: float) -> str:
		    return f"Hello, {name}! {int(count)}"
	'
}
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, World! 1'
