use ../../test.nu *

let temp_dir = mktemp -d

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default async () => {
			let bar = await tg.file("bar");
			return tg.directory({
				"foo": tg.file({
					contents: "foo",
					dependencies: {
						"bar": {
							item: bar,
							options: {
								id: bar.id,
								tag: "bar"
							}
						}
					}
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

# Also verify the lockfile structure.
let lockfile_path = $checkout_path | path join 'tangram.lock'
let lockfile = open $lockfile_path | from json
snapshot -n lockfile ($lockfile | to json -i 2)
