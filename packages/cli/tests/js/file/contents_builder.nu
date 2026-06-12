use ../../../test.nu *

# The builder's contents method sets the file's contents.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await (await tg.file().contents("body")).text;'
}

let output = tg build $path
snapshot $output '"body"'
