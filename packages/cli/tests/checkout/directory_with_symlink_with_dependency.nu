use ../../test.nu *

# Checking out a directory containing a symlink whose artifact is a dependency file, with dependencies enabled, materializes the directory and symlink on disk.

let tmp = mktemp --directory

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
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $path
snapshot --path $path
