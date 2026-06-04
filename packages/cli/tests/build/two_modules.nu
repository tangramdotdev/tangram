use ../../test.nu *

# A package whose root module imports and runs a command from a sibling module within the same package builds successfully.

let server = spawn

let path = artifact {
	tangram.ts: '
		import bar from "./bar.tg.ts";
		export default () => tg.run(bar);
	'
	bar.tg.ts: 'export default () => "Hello from bar"'
}

let output = tg build $path
snapshot $output
