use ../../test.nu *

# Checking in a package that depends on a nonexistent tag succeeds under --unsolved-dependencies, leaving the dependency unresolved.

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as a from "a/^1.2";
	'
}

let id = tg checkin --unsolved-dependencies $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata
