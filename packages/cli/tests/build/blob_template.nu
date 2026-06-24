use ../../test.nu *

# The tg.blob template literal strips the leading indentation from a multiline blob and the text matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return tg.blob`\n\tHello, World!\n`.then((b) => b.text); }
	'
}

let output = tg build $path
snapshot $output
