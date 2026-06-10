use ../../test.nu *

# A package can import a sibling package using the "source" import attribute to point at a relative path and run its default export.

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import bar from "bar" with { source: "../bar" };
			export default () => tg.run(bar);
		'
	}
	bar: {
		tangram.ts: 'export default () => "Hello from bar";'
	}
}

let output = tg build ($path | path join './foo')
snapshot $output
