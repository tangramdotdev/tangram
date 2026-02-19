use ../../test.nu *

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async () => {
			const file = tg.file("hello from artifact");
			return await tg.build`mkdir -m 755 ${tg.output} && ln -s ${file} ${tg.output}/link`.env(tg.build(busybox));
		};
	'
}

let id = tg build $path
let object = tg object get --blobs --depth=inf --pretty $id
snapshot --name object $object

tg cache $id
snapshot --name cache --path ($server.directory | path join "artifacts" | path join ($id | str trim))
