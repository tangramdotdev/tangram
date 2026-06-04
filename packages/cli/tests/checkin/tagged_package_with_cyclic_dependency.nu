use ../../test.nu *

# Checking in a package that imports a tagged package whose modules form a cycle resolves and writes the expected lockfile.

let server = spawn

# Tag the a dependency.
let a_path = artifact {
	tangram.ts: '
		import foo from "./foo.tg.ts";
	'
	foo.tg.ts: '
		import * as a from "./tangram.ts";
	'
}
tg tag a $a_path

let path = artifact {
	tangram.ts: '
		import a from "a";
	'
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
