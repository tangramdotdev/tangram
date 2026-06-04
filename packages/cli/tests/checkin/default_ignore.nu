use ../../test.nu *

# Checking in a package ignores the default-ignored entries such as the .DS_Store, .git, and .tangram entries.

let server = spawn

let path = artifact {
	.DS_Store: ''
	.git: {
		config: ''
	}
	.tangram: {
		config: ''
	}
	tangram.ts: ''
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
