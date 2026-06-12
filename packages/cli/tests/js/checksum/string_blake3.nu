use ../../../test.nu *

# tg.checksum computes a blake3 checksum of a string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.checksum("hello", "blake3");'
}

let output = tg build $path
snapshot $output '"blake3:ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f"'
