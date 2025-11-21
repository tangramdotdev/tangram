use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
	tangram.ts: '
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

let id = tg build $path
let checkout_path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $checkout_path
snapshot --path $checkout_path

let lockfile_path = $checkout_path | path join 'tangram.lock'
let lockfile = open $lockfile_path | from json
snapshot -n lockfile ($lockfile | to json -i 2)
