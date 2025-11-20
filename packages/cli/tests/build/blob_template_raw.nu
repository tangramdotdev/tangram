use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.Blob.raw`\n\tHello, World!\n`.then((b) => b.text());
	'
}

let output = tg build $path | complete
success $output
snapshot $output.stdout
