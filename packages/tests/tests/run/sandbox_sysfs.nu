use ../../lib/test.nu *

# A sandboxed process can read the host's sysfs, both with its own network namespace and with host networking.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
if not ('/sys/devices/system/cpu/possible' | path exists) { skip_test 'this test requires the possible CPU set' }

let expected = open --raw /sys/devices/system/cpu/possible | str trim

for kind in [builtin bubblewrap] {
	let backend = match $kind {
		builtin => { kind: builtin },
		bubblewrap => {
			if (which bwrap | is-empty) { continue }
			{ kind: bubblewrap, path: (which bwrap | first | get path) }
		},
	}
	let local = server spawn --directory (mktemp --directory) --config { runner: { backend: $backend } }
	for network in [none host] {
		let flags = if $network == host { [--network=host] } else { [] }
		let output = tg run --sandbox ...$flags --executable /bin/sh -- -c 'read -r possible < /sys/devices/system/cpu/possible && echo "$possible"' | complete
		success $output $"($kind) with network ($network): the process should read the possible CPU set"
		assert equal ($output.stdout | str trim) $expected
	}
	server stop $local
}
