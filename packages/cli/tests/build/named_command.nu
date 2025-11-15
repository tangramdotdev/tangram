use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export let five = () => 5;
		export let six = () => 6;
	'
}

let output = tg build ($path + '#five') | complete
success $output
snapshot $output.stdout
