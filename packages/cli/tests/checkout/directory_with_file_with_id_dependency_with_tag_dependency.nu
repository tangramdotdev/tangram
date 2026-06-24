use ../../test.nu *

# Checking out a directory containing a file with an id-keyed dependency that itself carries a tagged dependency, with dependencies enabled, materializes the directory and its nested dependencies on disk.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default async function () {
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
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $path
snapshot --path $path
