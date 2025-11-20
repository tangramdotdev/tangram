use ../../test.nu *

let server = spawn

# Tag the hello dependency.
let hello_path = artifact {
	contents: 'Hello, world!'
}
tg tag hello ($hello_path | path join 'contents')

let path = artifact {
	tangram.ts: 'import hello from "hello";'
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

# This should create a lockfile since it has a tagged dependency.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock ($lock | to json -i 2)
