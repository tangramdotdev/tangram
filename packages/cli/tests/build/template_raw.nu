use ../../test.nu *

# The tg.Template.raw template literal preserves the literal indentation and whitespace of its body and matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return tg.Template.raw`\n\tHello, World!\n`; }
	'
}

let output = tg build $path
snapshot $output
