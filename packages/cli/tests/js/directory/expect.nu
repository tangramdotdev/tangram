use ../../../test.nu *

# tg.Directory.expect returns the value unchanged when it is a directory.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.Directory.expect(await tg.directory({ "a": "alpha" })) instanceof tg.Directory; }'
}

let output = tg build $path
snapshot $output 'true'
