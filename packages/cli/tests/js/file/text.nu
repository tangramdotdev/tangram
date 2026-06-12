use ../../../test.nu *

# A file's text accessor returns its contents decoded as a string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await (await tg.file("hello")).text;'
}

let output = tg build $path
snapshot $output '"hello"'
