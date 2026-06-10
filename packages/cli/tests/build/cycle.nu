use ../../test.nu *

# A build whose command builds itself fails because it creates a process cycle.

let server = spawn

let path = artifact {
	tangram.ts: '
		export let x = () => tg.build(x);
	'
}

let output = tg build ($path + '#x') | complete
failure $output
