use ../../test.nu *

# The tg.File.raw template literal preserves the leading indentation and whitespace of a multiline file and the text matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return tg.File.raw`\n\tHello, World!\n`.then((f) => f.text); }
	'
}

let output = tg build $path
snapshot $output
