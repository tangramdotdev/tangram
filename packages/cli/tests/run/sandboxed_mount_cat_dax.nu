use ../lib/test.nu *

# A DAX sandbox can read an artifact through its mounted path.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if (which cloud-hypervisor | is-empty) {
	skip_test 'this test requires cloud-hypervisor'
}
let kernel_path = $env.TANGRAM_TEST_KERNEL_PATH? | default ''
if ($kernel_path | is-empty) {
	skip_test 'TANGRAM_TEST_KERNEL_PATH is required for cloud-hypervisor'
}

for dax in [true false] {
	let local = server spawn --busybox --config {
		runner: { backend: { kind: 'cloud-hypervisor', dax: $dax, kernel_path: $kernel_path } },
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
