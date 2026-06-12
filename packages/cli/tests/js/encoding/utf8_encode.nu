use ../../../test.nu *

# tg.encoding.utf8.encode encodes a string to its UTF-8 byte representation.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.encoding.utf8.encode("hi");'
}

let output = tg build $path
snapshot $output 'tg.bytes("aGk=")'
