use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
	tangram.ts: '
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

let id = tg build $path
let checkout_path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $checkout_path
snapshot --path $checkout_path
