use ../../util/test.nu *

# A stopped sandbox whose workload gave the directories that the server prepares to other identities starts again, and keeps the workload's other files and their owners.

default_id_maps | ignore
let local = server spawn --busybox

let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string, sandbox: string) => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox(sandbox);
	',
}
let sandbox = tg sandbox create --no-tokens | referent node
let script = r#'
	set -eu
	mkdir /data
	printf contents > /data/file
	chown -R 1:2 /data
	chmod 700 /data
	chown 3:4 / /opt /opt/tangram /tmp
	chmod 700 / /opt /opt/tangram /tmp
	echo written
'#
let output = tg run $command --arg-string $script --arg-string $sandbox | complete
success $output 'the workload should give the directories to other identities'
tg sandbox stop --wait $sandbox
tg sandbox start --wait $sandbox

let script = r#'
	set -eu
	stat -c %u:%g /data /data/file
	cat /data/file
	echo
	touch /tmp/file
'#
let output = tg run $command --arg-string $script --arg-string $sandbox | complete
success $output 'the restarted sandbox should run the workload'
assert equal ($output.stdout | lines) ['1:2' '1:2' 'contents']
tg sandbox destroy --wait $sandbox
