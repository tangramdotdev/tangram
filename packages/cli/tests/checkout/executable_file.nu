use ../../test.nu *

# Checking out an executable file materializes the file on disk with its executable bit preserved.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			return tg.file({
				contents: "Hello, World!",
				executable: true,
			})
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout $id $path
snapshot --path $path
