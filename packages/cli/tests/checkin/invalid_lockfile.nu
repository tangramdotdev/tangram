use ../../test.nu *

# Checking in a package with an invalid sibling lockfile ignores the lockfile, produces the expected object, and removes the lockfile.

let server = spawn

let path = artifact {
	tangram.lock: '
		{
			"nodes": [
				{
					"kind": "file"
				}
			]
		}
	'
	a: {
		tangram.ts: ''
	}
}

let id = tg checkin ($path | path join 'a')
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'a' 'tangram.lock'
assert (not ($lockfile_path | path exists))
