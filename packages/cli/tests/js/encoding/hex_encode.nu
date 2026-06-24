use ../../../test.nu *

# tg.encoding.hex.encode encodes a byte array to a lowercase hexadecimal string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.hex.encode(tg.encoding.utf8.encode("Hello")); }'
}

let output = tg build $path
snapshot $output '"48656c6c6f"'
