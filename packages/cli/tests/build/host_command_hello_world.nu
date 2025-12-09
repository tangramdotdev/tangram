use ../../test.nu *

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.run`echo "Hello, World!" > ${tg.output}`.env(tg.build(busybox));
	'
}

let id = tg build $path
let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object
