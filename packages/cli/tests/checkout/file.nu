use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("Hello, World!")
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
run tg checkout $id $path
snapshot --path $path
