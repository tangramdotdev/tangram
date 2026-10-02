use ../lib/test.nu *

# Checking in a package with a cyclic path dependency between two sibling packages succeeds and writes no lockfile.

let local = server spawn

let path = artifact {
	directory: {
		foo: {
			tangram.ts: 'import * as bar from "../bar";'
		}
		bar: {
			tangram.ts: 'import * as foo from "../foo";'
		}
	}
}

let id = tg checkin --no-tokens ($path | path join 'directory' 'foo') | referent node
tg index

let object = tg object get --blobs --depth=inf --no-tokens --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'directory' 'foo' 'tangram.lock'
assert (not ($lockfile_path | path exists))
