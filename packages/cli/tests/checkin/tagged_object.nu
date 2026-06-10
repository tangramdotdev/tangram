use ../../test.nu *

# Checking in a package that imports a tagged non-package object resolves the object and writes the expected lockfile.

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
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

# This should create a lockfile since it has a tagged dependency.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot --name lock ($lock | to json --indent 2)
