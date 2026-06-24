use ../../../test.nu *

# tg.encoding.base64.encode encodes a byte array to a standard padded base64 string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.base64.encode(tg.encoding.utf8.encode("Hello, World!")); }'
}

let output = tg build $path
snapshot $output '"SGVsbG8sIFdvcmxkIQ=="'
