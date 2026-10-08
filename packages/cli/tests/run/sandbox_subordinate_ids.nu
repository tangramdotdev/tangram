use ../lib/test.nu *

# Container sandboxes can map workload identities into subordinate host ranges.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if not (container_id_mapped_mount_supported) {
	skip_test 'this test requires ID-mapped mount support from the backing filesystem'
}

let maps = container_id_maps
let local = server spawn --busybox --config {
	runner: {
		isolation: {
			container: $maps,
		},
	},
}

let script = 'while IFS= read -r line; do echo "$line"; done < /proc/self/uid_map; while IFS= read -r line; do echo "$line"; done < /proc/self/gid_map'
let output = tg run --sandbox --executable /bin/sh -- -c $script | complete
success $output 'a sandbox with subordinate identity maps should succeed'
let lines = $output.stdout | lines | each {
	split row --regex '\s+' | where { not ($in | is-empty) } | each { into int }
}
assert equal ($lines | get 0) [0 $maps.uid_map.host $maps.uid_map.count]
assert equal ($lines | get 1) [$maps.uid_map.count (id -u | into int) 1]
assert equal ($lines | get 2) [0 $maps.gid_map.host $maps.gid_map.count]
assert equal ($lines | get 3) [$maps.gid_map.count (id -g | into int) 1]

# The runner can check in private files both during and after the workload.
let script = r#'
	set -eu
	umask 077
	mkdir -p "$TANGRAM_OUTPUT/nested"
	printf private > "$TANGRAM_OUTPUT/nested/file"
	chmod 700 "$TANGRAM_OUTPUT/nested/file"
	ln -s file "$TANGRAM_OUTPUT/nested/link"
	/opt/tangram/bin/tangram checkin "$TANGRAM_OUTPUT/nested/file" > /dev/null
	test "$(stat -c %a "$TANGRAM_OUTPUT/nested")" = 700
	test "$(stat -c %a "$TANGRAM_OUTPUT/nested/file")" = 700
	printf secret > "$TANGRAM_OUTPUT/private"
	test "$(stat -c %a "$TANGRAM_OUTPUT/private")" = 600
'#
let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string, network: string) => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox().network(network === "true");
	',
}
for network in ['false' 'true'] {
	let output = tg run $command --arg-string $script --arg-string $network | complete
	success $output 'the mapped workload should connect to the API and return private outputs'
	let id = $output.stdout | str trim
	let path = tg checkout $id | str trim
	assert equal (open --raw ($path | path join private)) 'secret'
	assert equal (open --raw ($path | path join nested file)) 'private'
	assert equal (open --raw ($path | path join nested link)) 'private'
	assert equal (^stat -c %a ($path | path join nested file) | str trim) '555'
}
