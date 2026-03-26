use ../../test.nu *

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			const host = tg.process.env.TANGRAM_HOST;
			tg.assert(typeof host === "string");
			return await tg.run({
				args: ["-c", tg`mkdir -p ${tg.output} && printf "Hello, World!\n" > ${tg.output}/message.txt`],
				executable: "sh",
				host,
				env: busybox(),
			});
		}
	',
}

let id = tg run $path
let object = tg object get --blobs --depth=inf --pretty $id
snapshot $object
