use ../../test.nu *

let server = spawn

# Tag the hello dependency.
let hello_path = artifact {
	contents: 'Hello, world!'
}
run tg tag hello ($hello_path | path join 'contents')

let path = artifact {
	tangram.ts: 'import hello from "hello";'
}

let id = run tg checkin $path
run tg index

let object = run tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = run tg object metadata --pretty $id
snapshot -n metadata $metadata

# This should create a lockfile since it has a tagged dependency.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock ($lock | to json -i 2)
