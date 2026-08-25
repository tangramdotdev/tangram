use ../../test.nu *

# A child sandbox cannot widen its parent's network or mount access.

let server = server spawn

let exact_path = artifact {
	tangram.ts: 'export default function () { return tg.build`echo hello`.network(); }',
}
failure (tg build $exact_path | complete)

def assert_denied [source: string, message: string] {
	let path = artifact {
		tangram.ts: $source,
	}
	let output = tg build $path | complete
	failure $output
	assert ($output.stderr | str contains $message) $'expected the error to contain "($message)", got: ($output.stderr)'
}

assert_denied '
	export default function () {
		return tg.run`echo hello`
			.sandbox()
			.network();
	}
' 'the child sandbox network is more permissive than the parent sandbox network'

assert_denied '
	export default function () {
		return tg.run`echo hello`
			.sandbox()
			.mount({ source: "/tmp", target: "/work" });
	}
' 'the child sandbox mount is more permissive than the parent sandbox mounts'

assert_denied '
	export default async function () { await tg.Sandbox.create().network(); }
' 'the child sandbox network is more permissive than the parent sandbox network'

let same_sandbox_path = artifact {
	tangram.ts: '
		export default async function (sandbox) {
			await tg.run`true`.sandbox(sandbox);
		}
	',
}
let sandbox = tg sandbox create --no-network | str trim
success (tg run $"--sandbox=($sandbox)" $same_sandbox_path --arg-string $sandbox | complete)

let different_sandbox_path = artifact {
	tangram.ts: '
		export default async function (sandbox) {
			await tg.run`true`.sandbox(sandbox);
		}
	',
}
let other_sandbox = tg sandbox create --no-network | str trim
let output = tg run $"--sandbox=($sandbox)" $different_sandbox_path --arg-string $other_sandbox | complete
failure $output
assert ($output.stderr | str contains 'the target sandbox does not match the request origin sandbox')

let network_path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`true`.sandbox().network();
		}
	',
}
success (tg run --sandbox --network=true $network_path | complete)

let checksum_path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run`true`
				.sandbox()
				.network()
				.checksum("sha256:any");
		}
	',
}
success (tg build $checksum_path | complete)

let mount = mktemp --directory | str trim
let readonly_mount_path = artifact {
	tangram.ts: '
		export default async function (source) {
			await tg.run`true`
				.sandbox()
				.mount({ source, target: "/child", readonly: true });
		}
	',
}
success (tg run --sandbox --mount $'($mount):/parent,ro' $readonly_mount_path --arg-string $mount | complete)

let readwrite_mount_path = artifact {
	tangram.ts: '
		export default async function (source) {
			await tg.run`true`.sandbox().mount({ source, target: "/child" });
		}
	',
}
let output = tg run --sandbox --mount $'($mount):/parent,ro' $readwrite_mount_path --arg-string $mount | complete
failure $output
assert ($output.stderr | str contains 'the child sandbox mount is more permissive than the parent sandbox mounts')
