use std assert
use ../../test.nu *

let temp_dir = mktemp -d

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default async () => {
			let dependency = await tg.file("bar");
			return tg.file({
				contents: "foo",
				dependencies: {
					[dependency.id]: dependency,
				}
			})
		}
	'
}

# Build.
let id = tg build $path

# Checkout without dependencies.
let checkout_path = $temp_dir | path join "checkout"
tg checkout --dependencies=false $id $checkout_path

assert (snapshot -n result --path $checkout_path)
