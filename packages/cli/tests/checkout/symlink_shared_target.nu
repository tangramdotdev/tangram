use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"hello.txt": "Hello, World!",
				"link1": tg.symlink("hello.txt"),
				"link2": tg.symlink("hello.txt")
			})
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
run tg checkout $id $path
snapshot --path $path
