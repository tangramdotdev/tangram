use ../../../test.nu *

# tg.checksum checksums the contents of a blob.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => tg.checksum(await tg.blob("hello"), "sha256");'
}

let output = tg build $path
snapshot $output '"sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"'
