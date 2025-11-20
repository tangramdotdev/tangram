use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import bar from "../bar";
			export default () => tg.build(bar);
			export let greeting = () => "foo";
		'
	}
	bar: {
		tangram.ts: '
			import * as foo from "../foo";
			export default () => tg.build(foo.greeting);
		'
	}
}

let output = tg build ($path | path join './foo') | complete
success $output
snapshot $output.stdout
