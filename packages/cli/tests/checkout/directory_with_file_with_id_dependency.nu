use std assert
use ../../test.nu *

let temp_dir = mktemp -d

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default async () => {
			let dependency = await tg.file("bar");
			return tg.directory({
				"foo": tg.file({
					contents: "foo",
					dependencies: {
						[dependency.id]: dependency,
					},
				})
			})
		}
	'
}

# Build.
let id = tg build $path

# Checkout with dependencies.
let checkout_path = $temp_dir | path join "checkout"
tg checkout --dependencies=true $id $checkout_path

assert (snapshot -n result --path $checkout_path)
