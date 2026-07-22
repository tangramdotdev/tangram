use ../../test.nu *

# A sandboxed process can read an artifact it references through the mount.

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn --busybox --config { vfs: true }

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			let data = tg.directory({ "hello.txt": tg.file("granted contents") });
			return await tg.run`cat ${data}/hello.txt`
				.env(tg.build(busybox))
				.sandbox();
		}
	',
}

let output = tg run $path | str trim
assert ($output == 'granted contents') $'expected the granted artifact to be readable, got: ($output)'
