use ../../test.nu *

# Checking in a directory containing files and relative symlinks produces the expected object and writes no lockfile.

let server = spawn

let path = artifact {
	hello.txt: 'Hello, world!'
	link: (symlink 'hello.txt')
	subdirectory: {
		sublink: (symlink '../link')
	}
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

let lockfile_path = $path | path join 'tangram.lock'
assert (not ($lockfile_path | path exists))
