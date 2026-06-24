use ../../../test.nu *

# tg.checksum checksums an artifact by its object representation, which differs from the checksum of its raw contents.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.checksum(await tg.file("hello"), "sha256"); }'
}

let output = tg build $path
snapshot $output '"sha256:4bc678d476f1906a0e3e5e84f9d02d34957f3913bd5e5eb35cd3efa28ff80f40"'
