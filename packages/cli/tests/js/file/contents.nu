use ../../../test.nu *

# A file's contents accessor returns a blob.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => (await (await tg.file("hello")).contents) instanceof tg.Blob;'
}

let output = tg build $path
snapshot $output 'true'
