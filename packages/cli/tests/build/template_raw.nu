use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.Template.raw`\n\tHello, World!\n`;
	'
}

let output = tg build $path | complete
success $output
snapshot $output.stdout
