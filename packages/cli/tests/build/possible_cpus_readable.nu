use ../lib/test.nu *

# A sandboxed process can read the host's possible CPU set, which programs read to size per-CPU resources.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
if not ('/sys/devices/system/cpu/possible' | path exists) { skip_test 'this test requires the possible CPU set' }

let expected = open --raw /sys/devices/system/cpu/possible | str trim
let path = artifact {
	tangram.ts: '
		export default () => tg.build`read -r possible < /sys/devices/system/cpu/possible && echo "$possible" > ${tg.output}`;
	',
}

for kind in [builtin bubblewrap] {
	let backend = match $kind {
		builtin => { kind: builtin },
		bubblewrap => {
			if (which bwrap | is-empty) { continue }
			{ kind: bubblewrap, path: (which bwrap | first | get path) }
		},
	}
	let local = server spawn --directory (mktemp --directory) --config { runner: { backend: $backend } }
	let output = tg build $path | complete
	success $output $"($kind): the build should read the possible CPU set"
	assert equal (tg read ($output.stdout | str trim) | str trim) $expected
	server stop $local
}
