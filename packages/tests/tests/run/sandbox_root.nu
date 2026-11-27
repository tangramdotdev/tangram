use ../../util/test.nu *

# A sandboxed process runs as root, whose home is /root, under every Linux backend, with the default identities and with only the server's identity mapped.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.run({
			executable: "/bin/sh",
			args: ["-c", "id -u; id -g; id -un; echo \"$HOME\""],
		}).env(tg.build(busybox)).sandbox();
	',
}
let backends = [
	{ kind: builtin }
	{ kind: builtin, subordinate_ids: false }
	...(if (which bwrap | is-empty) { [] } else { [{ kind: bubblewrap, path: (which bwrap | first | get path) }] })
	...(if (which runsc | is-empty) { [] } else { [{ kind: gvisor, path: (which runsc | first | get path), platform: systrap }] })
]
for backend in $backends {
	let local = server spawn --busybox --config { runner: { backend: $backend } }
	let output = tg run $command | complete
	success $output $'the sandboxed process should run with ($backend | to json --raw)'
	assert equal ($output.stdout | lines) ['0' '0' 'root' '/root'] $'the sandboxed process should be root with ($backend | to json --raw)'
	server stop $local
}
