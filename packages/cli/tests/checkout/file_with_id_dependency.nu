use ../../test.nu *

# Checking out a file with an id-keyed dependency, with dependencies disabled, materializes only the file on disk.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default async function () {
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
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout --dependencies=false $id $path
snapshot --path $path
