use ../../test.nu *

# Verify that a per-sandbox VFS grants command inputs and hides unrelated artifacts with ENOENT.
# The server creates principal-scoped mounts for Linux container and VM isolation when the VFS is enabled.

if $nu.os-info.name != 'linux' {
	return
}

let server = server spawn --busybox --config { vfs: true }

# Create a foreign artifact that the build never references.
let foreign = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({ "secret.txt": tg.file("secret contents") })
	'
}) | str trim

# Use the BusyBox input as a positive control while attempting to read the foreign artifact.
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function (id: string) {
			let path = `/opt/tangram/store/${id}/secret.txt`;
			return await tg.run`if cat "${path}" 2>/dev/null; then echo leaked; else echo denied; fi`
				.env(tg.build(busybox))
				.sandbox();
		}
	',
}

let output = tg run $path --arg-string $foreign | str trim
assert ($output == 'denied') $'expected the foreign artifact read to be denied, got: ($output)'
assert (not ($output | str contains 'secret')) 'the foreign artifact contents leaked through the vfs'
