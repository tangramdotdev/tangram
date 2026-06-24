use ../../../test.nu *

# tg.Symlink.expect returns the value unchanged when it is a symlink.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.Symlink.expect(await tg.symlink("a/b")) instanceof tg.Symlink; }'
}

let output = tg build $path
snapshot $output 'true'
