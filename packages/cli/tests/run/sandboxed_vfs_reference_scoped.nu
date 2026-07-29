use ../../test.nu *

# A sandboxed process reads an artifact it references but is denied a foreign artifact whose id it merely knows.

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn --busybox --config { vfs: true }

let foreign = tg build (artifact {
	tangram.ts: '
		export default () => tg.directory({ "secret.txt": tg.file("foreign contents") })
	'
}) | str trim

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function (foreign: string) {
			let granted = tg.directory({ "ours.txt": tg.file("ours") });
			let foreignPath = `/opt/tangram/artifacts/${foreign}/secret.txt`;
			return await tg.run`
				cat ${granted}/ours.txt
				if cat "${foreignPath}" 2>/dev/null; then echo leaked; else echo denied; fi
			`.env(tg.build(busybox)).sandbox();
		}
	',
}

let output = tg run $path --arg-string $foreign | str trim
assert ($output | str contains 'ours') $'expected the referenced artifact to be readable, got: ($output)'
assert ($output | str contains 'denied') $'expected the foreign artifact read to be denied, got: ($output)'
assert (not ($output | str contains 'leaked')) 'the foreign artifact was readable despite never being referenced'
assert (not ($output | str contains 'foreign contents')) 'the foreign artifact contents leaked through the vfs'
