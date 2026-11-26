use ../../util/test.nu *

# With the default identity maps disabled, a sandbox maps only the server's identity, so that its workload cannot use a second identity.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --busybox --config {
	runner: {
		backend: { kind: builtin, subordinate_ids: false },
	},
}

let script = 'while IFS= read -r line; do echo "$line"; done < /proc/self/uid_map; while IFS= read -r line; do echo "$line"; done < /proc/self/gid_map'
let output = tg run --sandbox --executable /bin/sh -- -c $script | complete
success $output 'a sandbox with a single identity should succeed'
let lines = $output.stdout | lines | each {
	split row --regex '\s+' | where { not ($in | is-empty) } | each { into int }
}
let uid = id -u | into int
let gid = id -g | into int
assert equal $lines [[$uid $uid 1] [$gid $gid 1]]

let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string) => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox();
	',
}
let script = 'set -eu; directory=$(mktemp -d); touch "$directory/file"; if chown 1:1 "$directory/file"; then echo changed; else echo unchanged; fi'
let output = tg run $command --arg-string $script | complete
success $output 'the workload should run'
assert equal ($output.stdout | str trim) 'unchanged'
