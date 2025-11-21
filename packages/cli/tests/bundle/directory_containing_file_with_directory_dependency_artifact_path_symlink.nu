use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let inner_dir = tg.directory({
				"a": tg.file("a"),
			});
			let dependency = tg.directory({
				"dep": tg.file("dependency"),
				"link": tg.symlink({
					artifact: inner_dir,
					path: "a"
				}),
			});
			let file = await tg.file({
				contents: "f",
				dependencies: {
					"dependency": {
						item: dependency
					},
				},
				executable: true,
			});
			let dir = tg.directory({
				"file": file,
				"link": tg.symlink("file")
			});
			return tg.bundle(dir);
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
