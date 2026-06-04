use ../../test.nu *

# Checking in a package with a tagged dependency and a source-path override under --no-source-dependencies resolves to the tag and writes the expected lockfile.

let server = spawn

# Tag the a dependency.
let a_path = artifact {
	tangram.ts: '
		export default () => "a";
	'
}
tg tag a $a_path

let path = artifact {
	tangram.ts: '
		import a from "a" with { source: "../a" };
		export default tg.command(async () => {
			return await a();
		});
	'
	a: {
		tangram.ts: '
			export default () => "a";
		'
	}
}

let id = tg checkin --no-source-dependencies $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata

# This should create a lockfile since it has a tagged dependency.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot --name lock ($lock | to json --indent 2)
