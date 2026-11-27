use ../../util/test.nu *

# Without configured identity maps, a sandbox maps the server's subordinate ranges, so that its workload can use other identities.

let maps = default_id_maps
let local = server spawn --busybox

# The sandbox maps the ranges, with the workload's root at the server's identities for a server that is not root.
let script = 'while IFS= read -r line; do echo "$line"; done < /proc/self/uid_map; while IFS= read -r line; do echo "$line"; done < /proc/self/gid_map'
let output = tg run --sandbox --executable /bin/sh -- -c $script | complete
success $output 'a sandbox with the default identity maps should succeed'
let lines = $output.stdout | lines | each {
	split row --regex '\s+' | where { not ($in | is-empty) } | each { into int }
}
let uid = id -u | into int
let gid = id -g | into int
if $maps.layout == 'rootless' {
	assert equal $lines [
		[0 $uid 1]
		[1 $maps.uid_map.host $maps.uid_map.count]
		[0 $gid 1]
		[1 $maps.gid_map.host $maps.gid_map.count]
	]
} else {
	assert equal $lines [
		[0 $maps.uid_map.host $maps.uid_map.count]
		[$maps.uid_map.count $uid 1]
		[0 $maps.gid_map.host $maps.gid_map.count]
		[$maps.gid_map.count $gid 1]
	]
}

# The workload gives a file to other identities and runs a process under another identity, whose files it owns.
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
	file=$(mktemp)
	chown 1:2 "$file"
	stat -c %u:%g "$file"
	chpst -u 3:4 sh -c 'id -u; id -g; touch /tmp/switched; stat -c %u:%g /tmp/switched'
'#
let output = tg run $command --arg-string $script | complete
success $output 'the workload should use other identities'
assert equal ($output.stdout | lines) ['1:2' '3' '4' '3:4']
