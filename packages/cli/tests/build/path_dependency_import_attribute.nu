use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import bar from "bar" with { local: "../bar" };
			export default () => tg.run(bar);
		'
	}
	bar: {
		tangram.ts: 'export default () => "Hello from bar";'
	}
}

let output = tg build ($path | path join './foo') | complete
success $output
snapshot $output.stdout
