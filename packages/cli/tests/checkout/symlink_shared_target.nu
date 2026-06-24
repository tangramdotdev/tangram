use ../../test.nu *

# Checking out a directory containing two symlinks that target the same sibling file materializes the directory and both symlinks on disk.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default function () {
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
tg checkout $id $path
snapshot --path $path
