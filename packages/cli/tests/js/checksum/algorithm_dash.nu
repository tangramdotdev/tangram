use ../../../test.nu *

# tg.Checksum.algorithm extracts the algorithm from a checksum that uses a dash separator.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.Checksum.algorithm("blake3-ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f");'
}

let output = tg build $path
snapshot $output '"blake3"'
