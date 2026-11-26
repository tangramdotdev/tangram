use ../../util/test.nu *

# Without configured identity maps, a sandbox maps the server's subordinate ranges, so that its workload can use a second identity.

let maps = default_id_maps
let local = server spawn --busybox

let script = 'while IFS= read -r line; do echo "$line"; done < /proc/self/uid_map; while IFS= read -r line; do echo "$line"; done < /proc/self/gid_map'
let output = tg run --sandbox --executable /bin/sh -- -c $script | complete
success $output 'a sandbox with the default identity maps should succeed'
let lines = $output.stdout | lines | each {
	split row --regex '\s+' | where { not ($in | is-empty) } | each { into int }
}
assert equal ($lines | get 0) [0 $maps.uid_map.host $maps.uid_map.count]
assert equal ($lines | get 1) [$maps.uid_map.count (id -u | into int) 1]
assert equal ($lines | get 2) [0 $maps.gid_map.host $maps.gid_map.count]
assert equal ($lines | get 3) [$maps.gid_map.count (id -g | into int) 1]

let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string) => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox();
	',
}
let script = 'set -eu; directory=$(mktemp -d); touch "$directory/file"; chown 1:1 "$directory/file"; stat -c %u:%g "$directory/file"'
let output = tg run $command --arg-string $script | complete
success $output 'the workload should change the owner of a file to a second identity'
assert equal ($output.stdout | str trim) '1:1'
