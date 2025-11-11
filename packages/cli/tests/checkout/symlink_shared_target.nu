use std assert
use ../../test.nu *

let temp_dir = mktemp -d

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default () => {
			return tg.directory({
				"hello.txt": "Hello, World!",
				"link1": tg.symlink("hello.txt"),
				"link2": tg.symlink("hello.txt")
			})
		}
	'
}

# Build.
let id = tg build $path

# Checkout without dependencies.
let checkout_path = $temp_dir | path join "checkout"
tg checkout $id $checkout_path

assert (snapshot -n result --path $checkout_path)
