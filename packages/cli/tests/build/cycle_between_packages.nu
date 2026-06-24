use ../../test.nu *

# Two packages whose builds invoke each other fail because they form a process cycle.

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import bar from "../bar";
			export default function () { return tg.build(bar); }
		'
	}
	bar: {
		tangram.ts: '
			import foo from "../foo";
			export default function () { return tg.build(foo); }
		'
	}
}

let output = tg build ($path | path join './foo') | complete
failure $output
