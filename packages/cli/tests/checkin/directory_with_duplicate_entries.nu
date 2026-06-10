use ../../test.nu *

# Checking in a directory whose entries have identical contents deduplicates them into a single blob.

let server = spawn

let path = artifact {
	a.txt: 'Hello, World!'
	b.txt: 'Hello, World!'
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
