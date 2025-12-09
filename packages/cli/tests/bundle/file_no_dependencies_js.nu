use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let file = await tg.file("hello");
			return tg.bundle(file);
		};
	'
}

# Build the module.
let id = tg build $path

# Checkout the artifact.
let temp_dir = mktemp -d
let checkout_path = $temp_dir | path join "checkout"
let output = tg checkout $id $checkout_path | complete

success $output

snapshot --path $checkout_path
