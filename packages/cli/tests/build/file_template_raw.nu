use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.File.raw`\n\tHello, World!\n`.then((f) => f.text());
	'
}

let output = tg build $path | complete
success $output
snapshot $output.stdout
