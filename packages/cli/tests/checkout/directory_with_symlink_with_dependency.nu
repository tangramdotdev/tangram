use std assert
use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default () => {
			return tg.directory({
				"foo": tg.symlink({artifact: tg.file("bar")})
			})
		}
	'
}

# Build.
let id = tg build $path

let checkout_path = $tmp | path join "checkout"

# Checkout with dependencies.
tg checkout --dependencies=true $id $checkout_path

assert (snapshot -n result --path $checkout_path)
