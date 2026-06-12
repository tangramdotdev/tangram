use ../../../test.nu *

# tg.checksum computes a sha256 checksum of a string, formatted as the algorithm and a lowercase hex digest.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.checksum("hello", "sha256");'
}

let output = tg build $path
snapshot $output '"sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"'
