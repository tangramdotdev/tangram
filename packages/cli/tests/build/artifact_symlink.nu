use ../../test.nu *

# A build can produce a symlink that points to an artifact and the resulting object and checkout match their snapshots.

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			const file = tg.file("hello from artifact");
			return await tg.build`mkdir -m 755 ${tg.output} && ln -s ${file} ${tg.output}/link`.env(tg.build(busybox));
		}
	'
}

let id = tg build $path
let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

tg checkout $id
snapshot --name checkout --path ($server.directory | path join "store" | path join ($id | str trim))
