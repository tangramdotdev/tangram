use ../../util/test.nu *

# With the store mounted through the VFS, a sandbox still maps the server's subordinate ranges, and a process under any of its identities runs programs from the store.

default_id_maps | ignore
let local = server spawn --busybox --config { vfs: true }

let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string) => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox();
	',
}
let script = r#'
	set -eu
	id -u
	chpst -u 65534:65534 sh -c 'id -u; cat "$(command -v sh)" > /dev/null; echo read'
'#
let output = tg run $command --arg-string $script | complete
success $output 'a process under another identity should run programs from the store'
assert equal ($output.stdout | lines) ['0' '65534' 'read']
