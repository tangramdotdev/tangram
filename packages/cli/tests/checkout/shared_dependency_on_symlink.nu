use std assert
use ../../test.nu *

let temp_dir = mktemp -d

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default async () => {
			let dependency = await tg.directory({
				"file.txt": "contents",
				"link": tg.symlink("file.txt"),
			});
			let id = dependency.id;
			return tg.directory({
				"foo.txt": tg.file("foo", { dependencies: { [id]: { item: dependency }}}),
				"bar.txt": tg.file("bar", { dependencies: { [id]: { item: dependency }}})
			});
		}
	'
}

# Build.
let id = tg build $path

# Checkout with dependencies.
let checkout_path = $temp_dir | path join "checkout"
tg checkout --dependencies=true $id $checkout_path

assert (snapshot -n result --path $checkout_path)
