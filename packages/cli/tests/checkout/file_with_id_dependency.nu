use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default async () => {
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
