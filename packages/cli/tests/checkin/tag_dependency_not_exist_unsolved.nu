use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as a from "a/^1.2";
	'
}

let id = tg checkin --unsolved-dependencies $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata
