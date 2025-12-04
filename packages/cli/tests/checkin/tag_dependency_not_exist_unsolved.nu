use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as a from "a/^1.2";
	'
}

let id = run tg checkin --unsolved-dependencies $path
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata
