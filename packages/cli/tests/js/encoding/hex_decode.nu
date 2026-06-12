use ../../../test.nu *

# tg.encoding.hex.decode decodes a lowercase hexadecimal string back to the original bytes.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.encoding.utf8.decode(tg.encoding.hex.decode("48656c6c6f"));'
}

let output = tg build $path
snapshot $output '"Hello"'
