use ../../test.nu *

# Building a package with a named export selects that export's command and returns its value.

let server = spawn

let path = artifact {
	tangram.ts: '
		export function five() { return 5; }
		export function six() { return 6; }
	'
}

let output = tg build ($path + '#five')
snapshot $output
