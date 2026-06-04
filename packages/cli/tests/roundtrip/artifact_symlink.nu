use ../../test.nu *

# Building a directory containing an artifact symlink, checking it out with dependencies, cleaning, and checking it back in yields the same artifact ID.

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			const directory = tg.directory();
			return tg.directory({
				directory: directory,
				link: tg.symlink({ artifact: directory }),
			});
		}
	'
}
let id = tg build $artifact

let tmp = mktemp --directory
let path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $path

tg clean

let left = tg checkin $path

assert equal $left $id
