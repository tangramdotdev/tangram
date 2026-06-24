use ../../../test.nu *

# tg.encoding.utf8.decode decodes a UTF-8 byte array, including multi-byte characters, to a string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.utf8.decode(new Uint8Array([0xe2, 0x9c, 0x93])); }'
}

let output = tg build $path
snapshot $output '"✓"'
