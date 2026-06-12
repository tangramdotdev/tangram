use ../../../test.nu *

# A file's length accessor returns the byte length of its contents.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await (await tg.file("hello")).length;'
}

let output = tg build $path
snapshot $output '5'
