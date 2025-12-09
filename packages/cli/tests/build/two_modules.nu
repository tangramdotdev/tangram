use ../../test.nu *

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
