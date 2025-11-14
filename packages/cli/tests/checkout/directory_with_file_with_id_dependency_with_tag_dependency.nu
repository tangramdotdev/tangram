use ../../test.nu *

let temp_dir = mktemp -d

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default async () => {
			let dependency = await tg.file("bar", { dependencies: { baz: tg.file("baz") } });
			return tg.directory({
				"foo": tg.file({
					contents: "foo",
					dependencies: {
						[dependency.id]: {
							item: dependency,
							options: {
								id: dependency.id
							}
						},
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

snapshot -n result --path $checkout_path
