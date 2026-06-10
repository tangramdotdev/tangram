use ../../test.nu *

# A build running a host shell command in a busybox environment writes to the output file and the resulting object matches the snapshot.

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build`echo "Hello, World!" > ${tg.output}`.env(tg.build(busybox));
	'
}

let id = tg build $path

let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object
