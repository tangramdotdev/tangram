use ../../../test.nu *

# tg.encoding.base64.decode decodes a base64 string back to the original bytes.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.utf8.decode(tg.encoding.base64.decode("SGVsbG8sIFdvcmxkIQ==")); }'
}

let output = tg build $path
snapshot $output '"Hello, World!"'
