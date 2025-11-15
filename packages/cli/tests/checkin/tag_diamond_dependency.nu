use ../../test.nu *

let server = spawn

# Tag the dependencies.
let d1_path = artifact {
	tangram.ts: '
		export default () => "d/1.0.0";
	'
}
tg tag d/1.0.0 $d1_path

let d11_path = artifact {
	tangram.ts: '
		export default () => "d/1.1.0";
	'
}
tg tag d/1.1.0 $d11_path

let b_path = artifact {
	tangram.ts: '
		import d from "d/^1";
		export default () => "b";
	'
}
tg tag b $b_path

let c_path = artifact {
	tangram.ts: '
		import d from "d/^1.0";
		export default () => "c";
	'
}
tg tag c $c_path

let path = artifact {
	tangram.ts: '
		import b from "b";
		import c from "c";
	'
}

let id = tg checkin $path
tg index

let object = tg object get --blobs --depth=inf --pretty $id
snapshot -n object $object

let metadata = tg object metadata --pretty $id
snapshot -n metadata $metadata

# This should create a lockfile since it has tagged dependencies.
let lockfile_path = $path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock ($lock | to json -i 2)
