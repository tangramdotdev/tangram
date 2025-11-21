use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default async () => {
			let dependency = await tg.file("bar");
			return tg.directory({
				"foo": tg.file({
					contents: "foo",
					dependencies: {
						[dependency.id]: dependency,
					},
				})
			})
		}
	'
}
let id = run tg build $artifact

let path = $tmp | path join "checkout"
run tg checkout --dependencies=true $id $path
snapshot --path $path
