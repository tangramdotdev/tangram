use ../../test.nu *

let server = spawn

# Tag the a dependency.
let a_path = artifact {
	tangram.ts: '
		export default () => "a";
	'
}
run tg tag a $a_path

let path = artifact {
	tangram.ts: '
		import a from "a";
		export default tg.command(async () => {
			return await a();
		});
	'
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
