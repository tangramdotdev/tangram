use ../../test.nu *

# The per-sandbox VFS mount enforces authorization. A sandboxed process can read
# the artifacts it is granted, which are its command's inputs delivered through
# the mount, but not a foreign artifact it was never granted. The provider hides
# the foreign artifact by returning ENOENT even though it exists in the store.
#
# Enforcement requires a principal-scoped per-sandbox mount, which the server
# starts for container and vm isolation when the vfs is enabled. On Linux the
# guest always sees the artifacts at /opt/tangram/artifacts.

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn --busybox --config { vfs: true }

# Create a foreign artifact that the build never references, so the build's
# process principal is not granted access to it.
let foreign = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({ "secret.txt": tg.file("secret contents") })
	'
}) | str trim

# The build reads the foreign artifact through the mount. The provider must deny
# the lookup with ENOENT, so the shell prints "denied" and never leaks the
# contents. The busybox environment is itself delivered through the mount, so a
# successful run is the positive control that granted content is readable.
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function (id: string) {
			let path = `/opt/tangram/artifacts/${id}/secret.txt`;
			return await tg.run`if cat "${path}" 2>/dev/null; then echo leaked; else echo denied; fi`
				.env(tg.build(busybox))
				.sandbox();
		}
	',
}

let output = tg run $path --arg-string $foreign | str trim
assert ($output == 'denied') $'expected the foreign artifact read to be denied, got: ($output)'
assert (not ($output | str contains 'secret')) 'the foreign artifact contents leaked through the vfs'
