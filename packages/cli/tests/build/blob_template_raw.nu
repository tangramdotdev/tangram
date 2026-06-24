use ../../test.nu *

# The tg.Blob.raw template literal preserves the leading indentation and whitespace of a multiline blob and the text matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return tg.Blob.raw`\n\tHello, World!\n`.then((b) => b.text); }
	'
}

let output = tg build $path
snapshot $output
