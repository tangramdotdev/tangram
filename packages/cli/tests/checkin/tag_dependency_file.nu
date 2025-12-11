use ../../test.nu *

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
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

# This should create a lockfile since it has a tagged dependency.
let lockfile_path = $path | path join 'package/tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock ($lock | to json -i 2)
