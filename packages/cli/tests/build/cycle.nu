use ../lib/test.nu *

# A build whose command builds itself fails because it creates a process cycle.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export function x() { return tg.build(x); }
	'
}

let output = tg build ($path + '#x') | complete
failure $output
