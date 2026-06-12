use ../../../test.nu *

# The builder's module method sets the file's module kind.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await (await tg.file("export default 1;").module("js")).module;'
}

let output = tg build $path
snapshot $output '"js"'
