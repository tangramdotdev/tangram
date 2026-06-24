use ../../test.nu *

# Building a module that bundles a file with no dependencies produces a checkout that matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let file = await tg.file("hello");
			return tg.bundle(file);
		}
	'
}

# Build the module.
let id = tg build $path

# Checkout the artifact.
let temp_dir = mktemp --directory
let checkout_path = $temp_dir | path join "checkout"
let output = tg checkout $id $checkout_path | complete

success $output

snapshot --path $checkout_path
