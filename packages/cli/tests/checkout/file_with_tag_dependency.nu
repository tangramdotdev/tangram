use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let bar = await tg.file("bar");
			return tg.file({
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
		}
	'
}

let id = tg build $path
let checkout_path = $tmp | path join "checkout"
tg checkout --dependencies=false $id $checkout_path
snapshot -n result --path $checkout_path
