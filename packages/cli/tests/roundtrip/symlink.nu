use ../../test.nu *

# Building a target symlink, checking it out with dependencies, cleaning, and checking it back in yields the same artifact ID.

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => tg.symlink("target");
	'
}
let id = tg build $artifact

let tmp = mktemp --directory
let path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $path

tg clean

let left = tg checkin $path

assert equal $left $id
