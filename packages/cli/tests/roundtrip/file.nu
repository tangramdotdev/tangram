use ../../test.nu *

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => tg.file("hello, world!");
	'
}
let id = run tg build $artifact

let tmp = mktemp -d
let path = $tmp | path join "checkout"
run tg checkout --dependencies=true $id $path

run tg clean

let left = run tg checkin $path

assert equal $left $id
