use ../../test.nu *

# Enumerating an authorized tag carries a target token into the sandbox VFS without exposing unrelated artifacts.

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn --busybox --config { vfs: true }

let target = tg checkin (artifact 'allowed') | str trim
let foreign = tg checkin (artifact 'foreign') | str trim
tg tag shared $target

let sandbox = tg sandbox create --no-network | str trim
tg grant $sandbox read shared | ignore
tg index

let module = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function (target: string, foreign: string) {
			let targetPath = `/opt/tangram/store/${target}`;
			let foreignPath = `/opt/tangram/store/${foreign}`;
			return await tg.run`
				ls /opt/tangram/store > /dev/null
				cat "${targetPath}"
				if cat "${foreignPath}" 2>/dev/null; then echo leaked; else echo denied; fi
			`.env(tg.build(busybox));
		}
	'
}

let output = (
	tg run $"--sandbox=($sandbox)" $module --arg-string $target --arg-string $foreign
	| str trim
)
assert ($output | str contains 'allowed') $'expected the tag target to be readable, got: ($output)'
assert ($output | str contains 'denied') $'expected the unrelated artifact to be denied, got: ($output)'
assert (not ($output | str contains 'leaked')) 'the unrelated artifact was readable through the vfs'
assert (not ($output | str contains 'foreign')) 'the unrelated artifact contents leaked through the vfs'
