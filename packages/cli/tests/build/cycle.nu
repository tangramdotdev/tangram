use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export let x = () => tg.build(x);
	'
}

let output = tg build ($path + '#x') | complete
failure $output
