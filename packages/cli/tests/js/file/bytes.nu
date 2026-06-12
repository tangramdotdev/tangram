use ../../../test.nu *

# A file's bytes accessor returns its contents as a byte array.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await (await tg.file("hello")).bytes;'
}

let output = tg build $path
snapshot $output 'tg.bytes("aGVsbG8=")'
