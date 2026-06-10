use ../../test.nu *

# Checking in a package that imports a file tagged dependency produces the expected object and writes the expected lockfile.

let server = spawn

let path = artifact {
	foo.tg.ts: ''
	package: {
		tangram.ts: '
			import "foo"
		'
	}
}

tg tag foo ($path | path join 'foo.tg.ts')

let id = tg checkin ($path | path join 'package')
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

# This should create a lockfile since it has a tagged dependency.
let lockfile_path = $path | path join 'package/tangram.lock'
let lock = open $lockfile_path | from json
snapshot --name lock ($lock | to json --indent 2)
