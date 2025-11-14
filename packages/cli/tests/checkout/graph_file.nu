use ../../test.nu *

let temp_dir = mktemp -d

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default () => {
			let graph = tg.graph({
				nodes: [{
					kind: "file",
					contents: "Hello, World!",
					executable: false,
				}],
			});
			return tg.file({ graph, node: 0 });
		};
	'
}

# Build.
let id = tg build $path

# Checkout without dependencies.
let checkout_path = $temp_dir | path join "checkout"
tg checkout $id $checkout_path

snapshot -n result --path $checkout_path
