use ../../test.nu *

# Checking out a directory containing a symlink that targets a sibling file materializes the directory and symlink on disk.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"hello.txt": "Hello, World!",
				"link": tg.symlink("hello.txt")
			})
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout $id $path
snapshot --path $path
