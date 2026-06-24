use ../../test.nu *

# Checking out a directory containing a file with a tagged dependency materializes the directory and writes the corresponding sibling lockfile.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default async function () {
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
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout $id $path
snapshot --path $path

let lockfile = open ($path | path join 'tangram.lock')
snapshot --name lockfile $lockfile
