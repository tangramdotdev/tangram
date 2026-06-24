use ../../../test.nu *

# tg.checksum computes a sha512 checksum of a string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.checksum("hello", "sha512"); }'
}

let output = tg build $path
snapshot $output '"sha512:9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"'
