use ../../test.nu *

if $nu.os-info.name != 'linux' {
	return
}

if (($env.TANGRAM_TEST_VM? | default "") | str length) == 0 {
	return
}

for dax in [true false] {
	let server = spawn --busybox --config {
		sandbox: {
			isolation: {
				vm: {
					dax: $dax,
				},
			},
		},
	}

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
}
