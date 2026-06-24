use ../../../test.nu *

# tg.Symlink.withId returns a symlink that preserves the given id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let symlink = await tg.symlink("a/b");
			return tg.Symlink.withId(symlink.id).id === symlink.id;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
