use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"foo": tg.symlink({artifact: tg.file("bar")})
			})
		}
	'
}
let id = run tg build $artifact

let path = $tmp | path join "checkout"
run tg checkout --dependencies=true $id $path
snapshot --path $path
