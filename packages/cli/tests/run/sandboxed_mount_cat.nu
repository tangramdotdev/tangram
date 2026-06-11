use ../../test.nu *

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn --busybox

let mount = mktemp -d | str trim
"hello from the mount\n" | save -f ($mount | path join "file")

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function (mount) {
			return await tg.run`cat /target/file`.env(tg.build(busybox)).sandbox().mount({ source: mount, target: "/target" });
		}
	',
}

let output = tg run $path --arg-string $mount | str trim
assert equal $output "hello from the mount"
