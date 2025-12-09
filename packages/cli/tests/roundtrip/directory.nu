use ../../test.nu *

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				file: tg.file("hello!"),
				link: tg.symlink("file"),
			});
		}
	'
}
let id = tg build $artifact

let tmp = mktemp -d
let path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $path

tg clean

let left = tg checkin $path

assert equal $left $id
