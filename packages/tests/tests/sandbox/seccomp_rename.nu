use ../../util/test.nu *

# A sandbox with the default seccomp policy can rename files, which libc does with renameat on architectures without a rename syscall.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build`
			cd "$(mktemp -d)"
			echo hello > source
			mv source destination
			cat destination > ${tg.output}
		`.env(tg.build(busybox));
	',
}

for kind in [builtin bubblewrap] {
	let backend = match $kind {
		builtin => { kind: builtin, seccomp: default },
		bubblewrap => {
			if (which bwrap | is-empty) { continue }
			{ kind: bubblewrap, path: (which bwrap | first | get path), seccomp: default }
		},
	}
	let local = server spawn --busybox --directory (mktemp --directory) --config { runner: { backend: $backend } }
	let output = tg build $path | complete
	success $output $"($kind): the build should rename a file"
	assert equal (tg read ($output.stdout | str trim) | str trim) hello
	server stop $local
}
